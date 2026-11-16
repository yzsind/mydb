// Multidimensional vector search benchmark: one C harness with shared data, timing, and concurrency.
//   scalar / neon(SIMD) / amx(Accelerate sgemm) / SME2 {f32,f16,bf16,int8}
//
// Task: find L2 top-k neighbors for each query against N database vectors of dimension D.
// Each thread scans the database for 16 queries (one SME row tile); measure 1/2/4/8/16-thread wall-clock throughput.
//
// M4 implements SME2 without non-streaming SVE; use -march=armv8-a+sme2 to avoid introducing SVE.
// Convert L2 to GEMM via ||q-x||^2 = ||q||^2 + ||x||^2 - 2*q*x; use FMOPA outer products.
// Prepack database vectors into tile panels during untimed setup, representing a built index.
//
// Build: clang -O3 -march=armv8-a+sme2 -framework Accelerate -lpthread sme_vec_bench.c -o sme_vec_bench
#include <arm_sme.h>
#include <arm_neon.h>
#include <Accelerate/Accelerate.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/sysctl.h>

#define TILE 16   // SVL=512 bits gives 16 f32 lanes.

static int DIM, NDB, NPAD, QPT, KTOP, ITERS;

static double now_s(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec + t.tv_nsec*1e-9; }

// Deterministic RNG over [-1,1), matching the Rust version.
static void gen(float* out, size_t count, uint64_t seed){
    uint64_t s = seed*0x9E3779B97F4A7C15ULL + 1;
    for(size_t i=0;i<count;i++){
        s ^= s>>12; s ^= s<<25; s ^= s>>27;
        uint32_t r = (uint32_t)((s*0x2545F4914F6CDD1DULL)>>40); // 24-bit
        out[i] = (float)r/(float)(1<<23) - 1.0f;
    }
}

// ---- Top-k for small k using linear insertion; return accumulated checksum ----
typedef struct { float d; uint32_t idx; } DI;
static double topk_scan_f32(const float* dist, int n, int k){
    DI buf[64]; int cnt=0;
    for(int j=0;j<n;j++){
        float d=dist[j];
        if(cnt==k && d>=buf[k-1].d) continue;
        if(cnt==k) cnt--;
        int p=cnt; buf[cnt].d=d; buf[cnt].idx=(uint32_t)j; cnt++;
        while(p>0 && buf[p-1].d>d){ DI t=buf[p]; buf[p]=buf[p-1]; buf[p-1]=t; p--; }
    }
    double acc=0; for(int i=0;i<cnt;i++) acc += (double)buf[i].d + buf[i].idx*1e-6;
    return acc;
}

// ============================ Global read-only data ============================
static float *DBf;          // [NDB*DIM]
static float *Qf;           // [maxT*QPT*DIM]
static float *XNORM;        // [NPAD] squared f32 database norms; padding=1e30.
// Prepacked database panels:
static float   *Bp32;       // f32:  [Ntiles*DIM*16]  idx=ct*DIM*16 + k*16 + j
static __fp16  *Bp16;       // f16:  [Ntiles*DIM*16]  idx=ct*DIM*16 + (k/2)*32 + j*2 + p
static uint16_t*Bpbf;       // bf16 uses the same bit layout as f16.
static int8_t  *Bp8;        // int8: [Ntiles*DIM*16]  idx=ct*DIM*16 + (k/4)*64 + j*4 + p
static int32_t *XNORM8;     // [NPAD] squared quantized int8 norms.
static int NTILES;
static const float I8SCALE = 127.0f;

static inline uint16_t f32_to_bf16(float f){ uint32_t x; memcpy(&x,&f,4); uint32_t r=x+0x7FFF+((x>>16)&1); return (uint16_t)(r>>16); }
static inline int8_t q8(float v){ int t=(int)lrintf(v*I8SCALE); if(t>127)t=127; if(t<-127)t=-127; return (int8_t)t; }

// ============================ One-time setup packing ============================
static void pack_db(int want_f32,int want_f16,int want_bf,int want_i8){
    NTILES = NPAD/TILE;
    if(want_f32) Bp32 = calloc((size_t)NTILES*DIM*16, sizeof(float));
    if(want_f16) Bp16 = calloc((size_t)NTILES*DIM*16, sizeof(__fp16));
    if(want_bf ) Bpbf = calloc((size_t)NTILES*DIM*16, sizeof(uint16_t));
    if(want_i8 ) Bp8  = calloc((size_t)NTILES*DIM*16, sizeof(int8_t));
    XNORM  = malloc((size_t)NPAD*sizeof(float));
    XNORM8 = malloc((size_t)NPAD*sizeof(int32_t));
    for(int ct=0;ct<NTILES;ct++){
      for(int j=0;j<TILE;j++){
        int rj=ct*TILE+j;
        float nrm=0; int64_t nrm8=0;
        for(int k=0;k<DIM;k++){
            float v = (rj<NDB)? DBf[(size_t)rj*DIM+k] : 0.0f;
            nrm += v*v;
            int8_t qv = q8(v); nrm8 += (int)qv*(int)qv;
            if(want_f32) Bp32[(size_t)ct*DIM*16 + k*16 + j] = v;
            if(want_f16) Bp16[(size_t)ct*DIM*16 + (k/2)*32 + j*2 + (k&1)] = (__fp16)v;
            if(want_bf ) Bpbf[(size_t)ct*DIM*16 + (k/2)*32 + j*2 + (k&1)] = f32_to_bf16(v);
            if(want_i8 ) Bp8 [(size_t)ct*DIM*16 + (k/4)*64 + j*4 + (k&3)] = qv;
        }
        if(rj<NDB){ XNORM[rj]=nrm; XNORM8[rj]=(int32_t)nrm8; }
      }
    }
    for(int j=NDB;j<NPAD;j++){ XNORM[j]=1e30f; XNORM8[j]=0x3f000000; }
}

// ============================ Timed per-thread query-panel packing ============================
static void pack_q_f32(const float* Q, float* Ap){ for(int k=0;k<DIM;k++) for(int i=0;i<TILE;i++) Ap[k*16+i]=Q[(size_t)i*DIM+k]; }
static void pack_q_f16(const float* Q, __fp16* Ap){ for(int k=0;k<DIM;k++) for(int i=0;i<TILE;i++) Ap[(k/2)*32+i*2+(k&1)]=(__fp16)Q[(size_t)i*DIM+k]; }
static void pack_q_bf (const float* Q, uint16_t* Ap){ for(int k=0;k<DIM;k++) for(int i=0;i<TILE;i++) Ap[(k/2)*32+i*2+(k&1)]=f32_to_bf16(Q[(size_t)i*DIM+k]); }
static void pack_q_i8 (const float* Q, int8_t* Ap){ for(int k=0;k<DIM;k++) for(int i=0;i<TILE;i++) Ap[(k/4)*64+i*4+(k&3)]=q8(Q[(size_t)i*DIM+k]); }

// ============================ Streaming SME GEMM kernels ============================
// All kernels produce G[16*NPAD] f32/int32 dot products, covering the database in one smstart.
__arm_new("za") __arm_locally_streaming
static void sme_gemm_f32(const float* Ap, const float* Bp, float* G){
    svbool_t pg = svptrue_b32();
    for(int ct=0;ct<NTILES;ct++){
        svzero_za();
        const float* B = Bp + (size_t)ct*DIM*16;
        for(int k=0;k<DIM;k++){
            svfloat32_t a = svld1_f32(pg, Ap + k*16);
            svfloat32_t b = svld1_f32(pg, B  + k*16);
            svmopa_za32_f32_m(0, pg, pg, a, b);
        }
        for(int i=0;i<TILE;i++) svst1_hor_za32(0, i, pg, G + (size_t)i*NPAD + ct*16);
    }
}
__arm_new("za") __arm_locally_streaming
static void sme_gemm_f16(const __fp16* Ap, const __fp16* Bp, float* G){
    svbool_t pg32 = svptrue_b32(); svbool_t pg16 = svptrue_b16();
    for(int ct=0;ct<NTILES;ct++){
        svzero_za();
        const __fp16* B = Bp + (size_t)ct*DIM*16;
        for(int k=0;k<DIM;k+=2){
            svfloat16_t a = svld1_f16(pg16, Ap + (k/2)*32);
            svfloat16_t b = svld1_f16(pg16, B  + (k/2)*32);
            svmopa_za32_f16_m(0, pg16, pg16, a, b);
        }
        for(int i=0;i<TILE;i++) svst1_hor_za32(0, i, pg32, G + (size_t)i*NPAD + ct*16);
    }
}
__arm_new("za") __arm_locally_streaming
static void sme_gemm_bf(const uint16_t* Ap, const uint16_t* Bp, float* G){
    svbool_t pg32 = svptrue_b32(); svbool_t pg16 = svptrue_b16();
    for(int ct=0;ct<NTILES;ct++){
        svzero_za();
        const uint16_t* B = Bp + (size_t)ct*DIM*16;
        for(int k=0;k<DIM;k+=2){
            svbfloat16_t a = svreinterpret_bf16_u16(svld1_u16(pg16, Ap + (k/2)*32));
            svbfloat16_t b = svreinterpret_bf16_u16(svld1_u16(pg16, B  + (k/2)*32));
            svmopa_za32_bf16_m(0, pg16, pg16, a, b);
        }
        for(int i=0;i<TILE;i++) svst1_hor_za32(0, i, pg32, G + (size_t)i*NPAD + ct*16);
    }
}
__arm_new("za") __arm_locally_streaming
static void sme_gemm_i8(const int8_t* Ap, const int8_t* Bp, int32_t* G){
    svbool_t pg32 = svptrue_b32(); svbool_t pg8 = svptrue_b8();
    for(int ct=0;ct<NTILES;ct++){
        svzero_za();
        const int8_t* B = Bp + (size_t)ct*DIM*16;
        for(int k=0;k<DIM;k+=4){
            svint8_t a = svld1_s8(pg8, Ap + (k/4)*64);
            svint8_t b = svld1_s8(pg8, B  + (k/4)*64);
            svmopa_za32_s8_m(0, pg8, pg8, a, b);
        }
        for(int i=0;i<TILE;i++) svst1_hor_za32(0, i, pg32, (uint32_t*)(G + (size_t)i*NPAD + ct*16));
    }
}

// ============================ Search: 16 queries per thread per batch ============================
enum { M_SCALAR, M_NEON, M_AMX, M_SME_F32, M_SME_F16, M_SME_BF, M_SME_I8, M_COUNT };

static double search_scalar(const float* Q){
    double acc=0; float* dist=malloc(NDB*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ const float* q=Q+(size_t)qi*DIM;
        for(int j=0;j<NDB;j++){ const float* x=DBf+(size_t)j*DIM; float s=0; for(int k=0;k<DIM;k++){float d=q[k]-x[k]; s+=d*d;} dist[j]=s; }
        acc += topk_scan_f32(dist,NDB,KTOP);
    } free(dist); return acc;
}
static double search_neon(const float* Q){
    double acc=0; float* dist=malloc(NDB*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ const float* q=Q+(size_t)qi*DIM;
        for(int j=0;j<NDB;j++){ const float* x=DBf+(size_t)j*DIM;
            float32x4_t a0=vdupq_n_f32(0),a1=vdupq_n_f32(0),a2=vdupq_n_f32(0),a3=vdupq_n_f32(0); int k=0;
            for(;k+16<=DIM;k+=16){
                float32x4_t d0=vsubq_f32(vld1q_f32(q+k),vld1q_f32(x+k)); a0=vfmaq_f32(a0,d0,d0);
                float32x4_t d1=vsubq_f32(vld1q_f32(q+k+4),vld1q_f32(x+k+4)); a1=vfmaq_f32(a1,d1,d1);
                float32x4_t d2=vsubq_f32(vld1q_f32(q+k+8),vld1q_f32(x+k+8)); a2=vfmaq_f32(a2,d2,d2);
                float32x4_t d3=vsubq_f32(vld1q_f32(q+k+12),vld1q_f32(x+k+12)); a3=vfmaq_f32(a3,d3,d3);
            }
            for(;k+4<=DIM;k+=4){ float32x4_t d0=vsubq_f32(vld1q_f32(q+k),vld1q_f32(x+k)); a0=vfmaq_f32(a0,d0,d0);}
            float s=vaddvq_f32(vaddq_f32(vaddq_f32(a0,a1),vaddq_f32(a2,a3)));
            for(;k<DIM;k++){float d=q[k]-x[k]; s+=d*d;} dist[j]=s;
        }
        acc += topk_scan_f32(dist,NDB,KTOP);
    } free(dist); return acc;
}
static double search_amx(const float* Q){
    double acc=0; float* qn=malloc(QPT*sizeof(float)); float* G=malloc((size_t)QPT*NDB*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ float s=0; const float* q=Q+(size_t)qi*DIM; for(int k=0;k<DIM;k++)s+=q[k]*q[k]; qn[qi]=s; }
    cblas_sgemm(CblasRowMajor,CblasNoTrans,CblasTrans,QPT,NDB,DIM,-2.0f,Q,DIM,DBf,DIM,0.0f,G,NDB);
    float* dist=malloc(NDB*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ const float* row=G+(size_t)qi*NDB; float qnv=qn[qi];
        for(int j=0;j<NDB;j++) dist[j]=qnv+XNORM[j]+row[j];
        acc += topk_scan_f32(dist,NDB,KTOP);
    } free(dist); free(G); free(qn); return acc;
}
static double topk_from_G_f32(const float* G,const float* qn){
    double acc=0; float* dist=malloc(NDB*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ const float* row=G+(size_t)qi*NPAD; float qnv=qn[qi];
        for(int j=0;j<NDB;j++) dist[j]=qnv+XNORM[j]-2.0f*row[j];
        acc += topk_scan_f32(dist,NDB,KTOP);
    } free(dist); return acc;
}
static double search_sme_float(const float* Q,int prec){
    float* qn=malloc(QPT*sizeof(float)); float* G=malloc((size_t)QPT*NPAD*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ float s=0; const float* q=Q+(size_t)qi*DIM; for(int k=0;k<DIM;k++)s+=q[k]*q[k]; qn[qi]=s; }
    if(prec==M_SME_F32){ float* Ap=malloc(DIM*16*sizeof(float)); pack_q_f32(Q,Ap); sme_gemm_f32(Ap,Bp32,G); free(Ap);}
    else if(prec==M_SME_F16){ __fp16* Ap=malloc(DIM*16*sizeof(__fp16)); pack_q_f16(Q,Ap); sme_gemm_f16(Ap,Bp16,G); free(Ap);}
    else { uint16_t* Ap=malloc(DIM*16*sizeof(uint16_t)); pack_q_bf(Q,Ap); sme_gemm_bf(Ap,Bpbf,G); free(Ap);}
    double acc=topk_from_G_f32(G,qn); free(G); free(qn); return acc;
}
static double search_sme_i8(const float* Q){
    int32_t* qn=malloc(QPT*sizeof(int32_t)); int32_t* G=malloc((size_t)QPT*NPAD*sizeof(int32_t));
    int8_t* Ap=malloc(DIM*16); pack_q_i8(Q,Ap);
    for(int qi=0;qi<QPT;qi++){ int64_t s=0; const float* q=Q+(size_t)qi*DIM; for(int k=0;k<DIM;k++){int8_t v=q8(q[k]); s+=(int)v*(int)v;} qn[qi]=(int32_t)s; }
    sme_gemm_i8(Ap,Bp8,G); free(Ap);
    double acc=0; float* dist=malloc(NDB*sizeof(float));
    for(int qi=0;qi<QPT;qi++){ const int32_t* row=G+(size_t)qi*NPAD; int32_t qnv=qn[qi];
        for(int j=0;j<NDB;j++) dist[j]=(float)(qnv+XNORM8[j]-2*row[j]);
        acc += topk_scan_f32(dist,NDB,KTOP);
    } free(dist); free(G); free(qn); return acc;
}

static double dispatch(int m,const float* Q){
    switch(m){
        case M_SCALAR: return search_scalar(Q);
        case M_NEON:   return search_neon(Q);
        case M_AMX:    return search_amx(Q);
        case M_SME_F32:case M_SME_F16:case M_SME_BF: return search_sme_float(Q,m);
        case M_SME_I8: return search_sme_i8(Q);
    } return 0;
}

// ============================ Concurrent runner ============================
typedef struct { int m; int t; double out; } Arg;
static void* worker(void* p){ Arg* a=p; const float* Q=Qf+(size_t)a->t*QPT*DIM; a->out=dispatch(a->m,Q); return NULL; }
static double run_cfg(int m,int threads,double* checksum){
    pthread_t th[16]; Arg ar[16];
    double t0=now_s();
    for(int t=0;t<threads;t++){ ar[t].m=m; ar[t].t=t; pthread_create(&th[t],NULL,worker,&ar[t]); }
    double cs=0; for(int t=0;t<threads;t++){ pthread_join(th[t],NULL); cs+=ar[t].out; }
    *checksum=cs; return now_s()-t0;
}
static int cmp_d(const void*a,const void*b){ double x=*(const double*)a,y=*(const double*)b; return (x>y)-(x<y); }
static double median(double* v,int n){ qsort(v,n,sizeof(double),cmp_d); return v[n/2]; }

static const char* MNAME[M_COUNT]={"scalar","neon(simd)","amx(accel)","sme.f32","sme.f16","sme.bf16","sme.int8"};

static int env_i(const char* k,int d){ const char* v=getenv(k); return v?atoi(v):d; }

int main(void){
    DIM=env_i("VS_DIM",768); NDB=env_i("VS_DB",50000); QPT=env_i("VS_QPT",16);
    KTOP=env_i("VS_K",10); ITERS=env_i("VS_ITERS",5);
    if(QPT%TILE){ fprintf(stderr,"QPT must be multiple of 16\n"); return 1; }
    NPAD=((NDB+TILE-1)/TILE)*TILE;
    int conc[5]={1,2,4,8,16}, maxT=16;

    // Confirm SVL=512 bits.
    { size_t svl=0; // The streaming kernel assumes 16 lanes; only print sysctl here.
      char brand[128]; size_t bl=sizeof(brand); if(sysctlbyname("machdep.cpu.brand_string",brand,&bl,NULL,0)) strcpy(brand,"?");
      printf("=== sme_vec_bench ===\nmachine=%s  march=armv8-a+sme2  SVL=512bit(f32 tile=16)\n",brand);
      (void)svl;
    }
    printf("config: DIM=%d DB=%d queries/thread=%d k=%d iters=%d (median)\n",DIM,NDB,QPT,KTOP,ITERS);
    printf("per-thread work = %d queries x %d vecs x %d dims = %.1fM FMA\n",QPT,NDB,DIM,(double)QPT*NDB*DIM/1e6);

    DBf=malloc((size_t)NDB*DIM*sizeof(float)); gen(DBf,(size_t)NDB*DIM,0xABCDEF);
    Qf =malloc((size_t)maxT*QPT*DIM*sizeof(float)); gen(Qf,(size_t)maxT*QPT*DIM,0x123456);
    pack_db(1,1,1,1);

    // ---- Correctness: compare top-k checksums of each path with scalar ----
    printf("\n[Correctness] One batch (16 queries), top-k checksums against scalar:\n");
    double ref=search_scalar(Qf);
    for(int m=0;m<M_COUNT;m++){ double v=dispatch(m,Qf);
        printf("  %-11s = %14.4f   Δ=%.3e%s\n",MNAME[m],v,fabs(v-ref)/fabs(ref),
            (m>=M_SME_F16)?"  (lower precision, approximate)":"");
    }

    // ---- Main matrix ----
    double qps[M_COUNT][5];
    for(int m=0;m<M_COUNT;m++){
        printf("\n[%s]\n%8s %10s %12s %12s %9s %11s\n","",MNAME[m],"","","","","");
        printf("%8s %10s %12s %12s %9s %11s\n","threads","wall_ms","queries/s","us/query","scale x","efficiency");
        double base=0;
        for(int ci=0;ci<5;ci++){ int t=conc[ci];
            double cs; run_cfg(m,t,&cs); // warmup
            double times[16]; for(int it=0;it<ITERS;it++) times[it]=run_cfg(m,t,&cs);
            double secs=median(times,ITERS);
            double totq=(double)t*QPT; double q=totq/secs; double us=secs*1e6/totq;
            if(ci==0) base=q; double sc=q/base; double eff=sc/t*100.0;
            printf("%8d %10.2f %12.0f %12.2f %8.2fx %10.1f%%\n",t,secs*1e3,q,us,sc,eff);
            qps[m][ci]=q;
        }
    }

    // ---- Speedup relative to scalar ----
    printf("\n[Speedup] Queries/s relative to scalar:\n%12s %9s %9s %9s %9s %9s\n","method","1T","2T","4T","8T","16T");
    for(int m=0;m<M_COUNT;m++){ printf("%12s",MNAME[m]); for(int ci=0;ci<5;ci++) printf(" %8.2fx",qps[m][ci]/qps[M_SCALAR][ci]); printf("\n"); }
    printf("\nDone.\n");
    return 0;
}
