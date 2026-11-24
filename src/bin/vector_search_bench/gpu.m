// ARC owns all Metal resources. Returned result leases outlive CPU refinement.
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#include <stdint.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

@interface MydbVectorGPU : NSObject
@property(nonatomic, strong) id<MTLDevice> device;
@property(nonatomic, strong) id<MTLCommandQueue> queue;
@property(nonatomic, strong) NSArray<id<MTLComputePipelineState>> *pipelines;
@property(nonatomic, strong) id<MTLBuffer> filterData;
@property(nonatomic, strong) MPSMatrix *database;
@property(nonatomic) NSUInteger dim;
@property(nonatomic) NSUInteger rows;
@property(nonatomic) NSUInteger paddedDim;
@property(nonatomic) NSUInteger paddedRows;
@property(nonatomic) BOOL useMPS;
@property(nonatomic) BOOL autoKernel;
@property(nonatomic) BOOL forceFilter;
@end
@implementation MydbVectorGPU
@end

static double now(void) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return value.tv_sec + value.tv_nsec * 1e-9;
}
static void failure(char *error, size_t capacity, NSString *message) {
    if (capacity) snprintf(error, capacity, "%s", message.UTF8String ?: "unknown Metal error");
}

int mydb_vector_gpu_available(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        return device != nil && device.hasUnifiedMemory && MPSSupportsMTLDevice(device);
    }
}

// A pipeline is immutable. Cache only this small device-specific object, never
// per-request buffers or query data; concurrent calls own independent storage.
static NSArray<id<MTLComputePipelineState>> *pipelines(id<MTLDevice> device, const char *source,
                                           NSError **error) {
    static NSArray<id<MTLComputePipelineState>> *cached;
    @synchronized([MydbVectorGPU class]) {
        if (cached && cached[0].device.registryID == device.registryID) return cached;
        if (![device supportsFamily:MTLGPUFamilyApple7]) return nil;
        MTLCompileOptions *options = [MTLCompileOptions new];
        if (@available(macOS 15.0, *)) {
            options.mathMode = MTLMathModeSafe;
        } else {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
            options.fastMathEnabled = NO;
#pragma clang diagnostic pop
        }
        id<MTLLibrary> library = [device newLibraryWithSource:[NSString stringWithUTF8String:source]
                                                    options:options error:error];
        if (!library) return nil;
        NSMutableArray<id<MTLComputePipelineState>> *states = [NSMutableArray new];
        id<MTLComputePipelineState> tiled = [device newComputePipelineStateWithFunction:
            [library newFunctionWithName:@"vector_dots"] error:error];
        if (!tiled) return nil;
        [states addObject:tiled];
        for (uint32_t width = 1; width <= 8; width *= 2) {
            MTLFunctionConstantValues *constants = [MTLFunctionConstantValues new];
            [constants setConstantValue:&width type:MTLDataTypeUInt atIndex:0];
            id<MTLFunction> function = [library newFunctionWithName:@"vector_filter"
                constantValues:constants error:error];
            if (!function) return nil;
            id<MTLComputePipelineState> state = [device newComputePipelineStateWithFunction:function error:error];
            if (!state) return nil;
            [states addObject:state];
        }
        for (id<MTLComputePipelineState> state in states) {
            if (state.threadExecutionWidth != 32 || state.maxTotalThreadsPerThreadgroup < 128) return nil;
        }
        cached = [states copy];
        return cached;
    }
}

void *mydb_vector_gpu_create(const float *db, size_t rows, size_t dim,
                             const char *source, uint32_t use_mps,
                             char *name, size_t name_capacity, size_t *bytes, size_t *filter_bytes,
                             char *error, size_t error_capacity) {
    @autoreleasepool {
        @try {
            MydbVectorGPU *context = [MydbVectorGPU new];
            context.device = MTLCreateSystemDefaultDevice();
            if (!context.device || !context.device.hasUnifiedMemory || !MPSSupportsMTLDevice(context.device)) {
                failure(error, error_capacity, @"a unified-memory Metal/MPS device is unavailable");
                return NULL;
            }
            context.queue = [context.device newCommandQueue];
            context.dim = dim;
            context.rows = rows;
            context.useMPS = use_mps == 1;
            context.autoKernel = use_mps == 2;
            context.forceFilter = use_mps == 3;
            *filter_bytes = 0;
            context.paddedDim = (dim + 7) / 8 * 8;
            context.paddedRows = (rows + 31) / 32 * 32;
            NSUInteger stride = context.paddedDim * sizeof(float);
            if (!context.queue || !stride || context.paddedRows > context.device.maxBufferLength / stride ||
                context.paddedDim > UINT32_MAX || context.paddedRows > UINT32_MAX) {
                failure(error, error_capacity, @"database dimensions exceed Metal buffer limits");
                return NULL;
            }
            if (!context.useMPS) {
                NSError *compileError = nil;
                context.pipelines = pipelines(context.device, source, &compileError);
                if (!context.pipelines) {
                    failure(error, error_capacity, compileError.localizedDescription ?:
                        @"SIMD-group FP32 matrix pipeline unavailable (VS_GPU_KERNEL=mps is supported)");
                    return NULL;
                }
            }
            id<MTLBuffer> buffer = [context.device newBufferWithLength:context.paddedRows * stride
                                                             options:MTLResourceStorageModeShared];
            if (!buffer) {
                failure(error, error_capacity, @"failed to allocate the Metal database buffer");
                return NULL;
            }
            // Explicitly initialize both dimension padding and final row tiles.
            memset(buffer.contents, 0, buffer.length);
            for (NSUInteger row = 0; row < rows; ++row) {
                memcpy((char *)buffer.contents + row * stride, db + row * dim, dim * sizeof(float));
            }
            // Logical MPS extents stay exact so its single-query/GEMV path is
            // preserved. The underlying buffer also has safe SIMD tile padding.
            MPSMatrixDescriptor *desc = [MPSMatrixDescriptor matrixDescriptorWithRows:context.rows
                columns:context.dim rowBytes:stride dataType:MPSDataTypeFloat32];
            context.database = [[MPSMatrix alloc] initWithBuffer:buffer descriptor:desc];
            if (!context.database) {
                failure(error, error_capacity, @"failed to describe the Metal database matrix");
                return NULL;
            }
            if (context.autoKernel || context.forceFilter) {
                NSUInteger halfStride = context.paddedDim * sizeof(__fp16);
                id<MTLBuffer> compact = [context.device newBufferWithLength:context.paddedRows * halfStride
                                                                   options:MTLResourceStorageModeShared];
                if (!compact) {
                    failure(error, error_capacity, @"failed to allocate the GPU filter representation");
                    return NULL;
                }
                memset(compact.contents, 0, compact.length);
                BOOL eligible = YES;
                for (NSUInteger row = 0; row < rows; ++row) {
                    __fp16 *target = (__fp16 *)((char *)compact.contents + row * halfStride);
                    for (NSUInteger k = 0; k < dim; ++k) {
                        float value = db[row * dim + k];
                        BOOL valid = isfinite(value) && fabsf(value) <= 65504.0f;
                        eligible &= valid;
                        // Flush tiny inputs explicitly. The CPU error bound
                        // covers this loss, independent of GPU half FTZ rules.
                        target[k] = valid && fabsf(value) >= 0x1p-14f ? (__fp16)value : (__fp16)0;
                    }
                }
                if (eligible) {
                    context.filterData = compact;
                    *filter_bytes = compact.length;
                }
            }
            *bytes = buffer.length;
            failure(name, name_capacity, context.device.name);
            return (__bridge_retained void *)context;
        } @catch (NSException *exception) {
            failure(error, error_capacity, exception.reason);
            return NULL;
        }
    }
}

void mydb_vector_gpu_release(void *opaque) {
    @autoreleasepool {
        id object = (__bridge_transfer id)opaque;
        (void)object;
    }
}

// On success the caller owns a retained Metal buffer. Its shared-memory result
// remains valid until release; no CPU copy and no second result allocation.
void *mydb_vector_gpu_dots(void *opaque, const float *queries, size_t count,
                           const float **dots, size_t *row_stride, double *stats,
                           char *error, size_t error_capacity) {
    @autoreleasepool {
        @try {
            double begin = now();
            MydbVectorGPU *context = (__bridge MydbVectorGPU *)opaque;
            BOOL useFilter = context.filterData && (context.forceFilter || (context.autoKernel && count <= 16));
            if (useFilter) {
                for (NSUInteger i = 0; i < count * context.dim; ++i) {
                    if (!isfinite(queries[i]) || fabsf(queries[i]) > 65504.0f) { useFilter = NO; break; }
                }
            }
            // Out-of-range/nonfinite inputs keep the original FP32 backend.
            BOOL useMPS = context.useMPS || (!useFilter && (context.autoKernel || context.forceFilter) && count == 1);
            NSUInteger filterIndex = count <= 1 ? 1 : count <= 2 ? 2 : count <= 4 ? 3 : 4;
            NSUInteger tileQueries = useFilter ? (1u << (filterIndex - 1)) : count <= 8 ? 8 : 16;
            NSUInteger paddedQueries = useMPS ? count : (count + tileQueries - 1) / tileQueries * tileQueries;
            NSUInteger qstride = context.paddedDim * (useFilter ? sizeof(__fp16) : sizeof(float));
            NSUInteger rstride = useMPS ? [MPSMatrixDescriptor rowBytesFromColumns:context.rows dataType:MPSDataTypeFloat32] : context.paddedRows * sizeof(float);
            if (count > UINT32_MAX || paddedQueries > context.device.maxBufferLength / qstride ||
                paddedQueries > context.device.maxBufferLength / rstride) {
                failure(error, error_capacity, @"query dimensions exceed Metal buffer limits");
                return NULL;
            }
            id<MTLBuffer> qbuffer = [context.device newBufferWithLength:paddedQueries * qstride
                                                              options:MTLResourceStorageModeShared];
            id<MTLBuffer> result = [context.device newBufferWithLength:paddedQueries * rstride
                                                             options:MTLResourceStorageModeShared];
            if (!qbuffer || !result) {
                failure(error, error_capacity, @"failed to allocate Metal query/result buffers");
                return NULL;
            }
            memset(qbuffer.contents, 0, qbuffer.length);
            for (NSUInteger row = 0; row < count; ++row) {
                if (useFilter) {
                    __fp16 *target = (__fp16 *)((char *)qbuffer.contents + row * qstride);
                    for (NSUInteger k = 0; k < context.dim; ++k) {
                        float value = queries[row * context.dim + k];
                        target[k] = fabsf(value) >= 0x1p-14f ? (__fp16)value : (__fp16)0;
                    }
                } else {
                    memcpy((char *)qbuffer.contents + row * qstride,
                           queries + row * context.dim, context.dim * sizeof(float));
                }
            }
            double allocated = now();
            id<MTLCommandBuffer> command = [context.queue commandBuffer];
            if (!command) {
                failure(error, error_capacity, @"failed to create a Metal command buffer");
                return NULL;
            }
            if (useMPS) {
                MPSMatrixDescriptor *qd = [MPSMatrixDescriptor matrixDescriptorWithRows:count
                    columns:context.dim rowBytes:qstride dataType:MPSDataTypeFloat32];
                MPSMatrixDescriptor *rd = [MPSMatrixDescriptor matrixDescriptorWithRows:count
                    columns:context.rows rowBytes:rstride dataType:MPSDataTypeFloat32];
                MPSMatrix *qm = [[MPSMatrix alloc] initWithBuffer:qbuffer descriptor:qd];
                MPSMatrix *rm = [[MPSMatrix alloc] initWithBuffer:result descriptor:rd];
                MPSMatrixMultiplication *kernel = [[MPSMatrixMultiplication alloc]
                    initWithDevice:context.device transposeLeft:NO transposeRight:YES
                    resultRows:count resultColumns:context.rows interiorColumns:context.dim
                    alpha:1.0 beta:0.0];
                if (!qm || !rm || !kernel) {
                    failure(error, error_capacity, @"failed to create the MPS multiplication");
                    return NULL;
                }
                [kernel encodeToCommandBuffer:command leftMatrix:qm rightMatrix:context.database resultMatrix:rm];
            } else {
                id<MTLComputeCommandEncoder> encoder = [command computeCommandEncoder];
                if (!encoder) {
                    failure(error, error_capacity, @"failed to create a Metal compute encoder");
                    return NULL;
                }
                [encoder setComputePipelineState:context.pipelines[useFilter ? filterIndex : 0]];
                [encoder setBuffer:qbuffer offset:0 atIndex:0];
                [encoder setBuffer:useFilter ? context.filterData : context.database.data offset:0 atIndex:1];
                [encoder setBuffer:result offset:0 atIndex:2];
                uint32_t shape[4] = {(uint32_t)context.paddedDim, (uint32_t)context.paddedRows,
                                     (uint32_t)count, 0};
                [encoder setBytes:shape length:sizeof(shape) atIndex:3];
                NSUInteger tileRows = useFilter ? 4 : count <= 8 ? 32 : 16;
                [encoder dispatchThreadgroups:MTLSizeMake(context.paddedRows / tileRows,
                                                        paddedQueries / tileQueries, 1)
                       threadsPerThreadgroup:MTLSizeMake(128, 1, 1)];
                [encoder endEncoding];
            }
            double encoded = now();
            [command commit];
            [command waitUntilCompleted];
            if (command.status != MTLCommandBufferStatusCompleted) {
                failure(error, error_capacity, command.error.localizedDescription ?: @"Metal command did not complete");
                return NULL;
            }
            stats[0] = allocated - begin;
            stats[1] = encoded - allocated;
            stats[2] = now() - encoded;
            stats[3] = command.GPUEndTime - command.GPUStartTime;
            stats[4] = useFilter ? 1.0 : 0.0;
            *dots = result.contents;
            *row_stride = rstride / sizeof(float);
            return (__bridge_retained void *)result;
        } @catch (NSException *exception) {
            failure(error, error_capacity, exception.reason);
            return NULL;
        }
    }
}
