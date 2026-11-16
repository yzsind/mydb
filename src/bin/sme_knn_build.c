// Prototype tiled brute-force SME kNN graph bootstrap, compared with incremental HNSW construction (link_all).
//
// Treat all N nodes as queries in 16-node tiles and use SME int8 GEMM against the full database.
// Reconstruct SQ8 L2 distances and select exact top-K neighbors, excluding self: an exact O(N^2) kNN graph.
// Timing includes quantization (like HNSW PhaseB arena), tile packing, GEMM, and topK, partitioned across threads.
// Recall: greedy best-first search on the flat kNN graph (ef=128, SQ8 distance), using 200 new queries.
// Compare with exact f32 top-10 to measure recall@10 and test whether an exact kNN graph is navigable.
//
// Build: clang -O3 -march=armv8.6-a+sme2+dotprod -lpthread src/bin/sme_knn_build.c -o sme_knn_build
// +dotprod provides search-side NEON vdotq_s32; +sme2 provides svmopa ZA-tile int8 GEMM.
// Run: BUILD_DIM=768 BUILD_SIZES="5000,10000,20000,50000,100000" ./sme_knn_build
// Compare against production NEON HNSW construction: scripts-internal/run_hnsw_sme_build_bench.sh.
#include <arm_sme.h>
#include <arm_neon.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/sysctl.h>

#define TILE 16
#define KGRAPH 32     // kNN graph out-degree, approximately HNSW 2*M.
#define EF 128
#define NQ 200
#define KTOP 10
static const float I8SCALE = 127.0f;

static int DIM;
static double now_s(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
static void gen(float* o,size_t c,uint64_t s){ s=s*0x9E3779B97F4A7C15ULL+1; for(size_t i=0;i<c;i++){s^=s>>12;s^=s<<25;s^=s>>27; uint32_t r=(uint32_t)((s*0x2545F4914F6CDD1DULL)>>40); o[i]=(float)r/(float)(1<<23)-1.0f;} }
static inline int8_t q8(float v){ int t=(int)lrintf(v*I8SCALE); if(t>127)t=127; if(t<-127)t=-127; return (int8_t)t; }

// ---- SME int8 GEMM:G[16*NPAD] = block(16) · DB^T ----
__arm_new("za") __arm_locally_streaming
static void sme_gemm_i8(const int8_t* Ap,const int8_t* Bp,int32_t* G,int NPAD,int NTILES){
    svbool_t pg32=svptrue_b32(), pg8=svptrue_b8();
    for(int ct=0;ct<NTILES;ct++){
        svzero_za();
        const int8_t* B=Bp+(size_t)ct*DIM*16;
        for(int k=0;k<DIM;k+=4){
            svint8_t a=svld1_s8(pg8,Ap+(k/4)*64);
            svint8_t b=svld1_s8(pg8,B +(k/4)*64);
            svmopa_za32_s8_m(0,pg8,pg8,a,b);
        }
        for(int i=0;i<TILE;i++) svst1_hor_za32(0,i,pg32,(uint32_t*)(G+(size_t)i*NPAD+ct*16));
    }
}

// ---- Global build state ----
static int N,NPAD,NTILES;
static int8_t *CODES;   // [N*DIM] contiguous int8 values per node for search dot products.
static int8_t *Bp8;     // [NTILES*DIM*16] packed database panels for GEMM.
static int32_t *XN8;    // [NPAD] squared int8 norms.
static uint32_t *KID;   // [N*KGRAPH] neighbor IDs.
static float    *KD;    // [N*KGRAPH] neighbor distances in ascending order.

static void pack_db(void){
    NTILES=NPAD/TILE;
    Bp8=calloc((size_t)NTILES*DIM*16,1);
    for(int ct=0;ct<NTILES;ct++) for(int j=0;j<TILE;j++){ int rj=ct*TILE+j;
        for(int k=0;k<DIM;k++){ int8_t v=(rj<N)?CODES[(size_t)rj*DIM+k]:0; Bp8[(size_t)ct*DIM*16+(k/4)*64+j*4+(k&3)]=v; }
    }
}
static void pack_qblock(int base,int8_t* Ap){
    for(int i=0;i<TILE;i++){ int rn=base+i;
        for(int k=0;k<DIM;k++){ int8_t v=(rn<N)?CODES[(size_t)rn*DIM+k]:0; Ap[(k/4)*64+i*4+(k&3)]=v; }
    }
}

// ---- Build worker: process query blocks [t::stride] ----
typedef struct{ int t,stride; } BArg;
static void* build_worker(void* p){
    BArg* a=p;
    int32_t* G=malloc((size_t)TILE*NPAD*sizeof(int32_t));
    int8_t* Ap=malloc(DIM*16);
    for(int ct=a->t; ct<NTILES; ct+=a->stride){
        int base=ct*TILE;
        pack_qblock(base,Ap);
        sme_gemm_i8(Ap,Bp8,G,NPAD,NTILES);
        for(int i=0;i<TILE;i++){ int node=base+i; if(node>=N) break;
            int32_t qn=XN8[node]; const int32_t* row=G+(size_t)i*NPAD;
            float* kd=KD+(size_t)node*KGRAPH; uint32_t* ki=KID+(size_t)node*KGRAPH;
            int cnt=0; float worst=INFINITY;
            for(int j=0;j<N;j++){ if(j==node) continue;
                float d=(float)(qn+XN8[j]-2*row[j]);
                if(cnt==KGRAPH && d>=worst) continue;
                int p2; if(cnt<KGRAPH){ p2=cnt; kd[cnt]=d; ki[cnt]=j; cnt++; }
                else { p2=KGRAPH-1; kd[p2]=d; ki[p2]=j; }
                while(p2>0 && kd[p2-1]>kd[p2]){ float td=kd[p2];kd[p2]=kd[p2-1];kd[p2-1]=td; uint32_t ti=ki[p2];ki[p2]=ki[p2-1];ki[p2-1]=ti; p2--; }
                worst=kd[cnt<KGRAPH?cnt-1:KGRAPH-1];
            }
        }
    }
    free(G); free(Ap); return NULL;
}

// ---- Search int8 dot product, using NEON dotprod when available ----
static inline int32_t dot_i8(const int8_t* a,const int8_t* b,int n){
    int32x4_t acc=vdupq_n_s32(0); int i=0;
    for(;i+16<=n;i+=16){ int8x16_t x=vld1q_s8(a+i),y=vld1q_s8(b+i);
        acc=vdotq_s32(acc,x,y); }
    int32_t s=vaddvq_s32(acc); for(;i<n;i++) s+=(int)a[i]*(int)b[i]; return s;
}

// ---- Greedy best-first search on a flat kNN graph using SQ8 distances ----
typedef struct{ float d; int id; } DI;
static void mheap_push(DI* h,int* n,float d,int id){ int i=(*n)++; h[i].d=d;h[i].id=id; while(i>0){int p=(i-1)/2; if(h[p].d<=h[i].d)break; DI t=h[p];h[p]=h[i];h[i]=t; i=p;} }
static DI mheap_pop(DI* h,int* n){ DI r=h[0]; h[0]=h[--(*n)]; int i=0; while(1){int l=2*i+1,rr=2*i+2,s=i; if(l<*n&&h[l].d<h[s].d)s=l; if(rr<*n&&h[rr].d<h[s].d)s=rr; if(s==i)break; DI t=h[s];h[s]=h[i];h[i]=t; i=s;} return r; }
static int search_flat(const int8_t* qc,int32_t qn,uint8_t* vis,DI* frontier,DI* res){
    memset(vis,0,N);
    int fn=0,rn=0; int entry=0;
    float d0=(float)(qn+XN8[entry]-2*dot_i8(qc,CODES+(size_t)entry*DIM,DIM));
    mheap_push(frontier,&fn,d0,entry); res[rn++]=(DI){d0,entry}; vis[entry]=1;
    // res is unordered; find its worst element linearly.
    while(fn>0){
        DI c=mheap_pop(frontier,&fn);
        float worst=-1; int wi=0; for(int i=0;i<rn;i++) if(res[i].d>worst){worst=res[i].d;wi=i;}
        if(rn>=EF && c.d>worst) break;
        const uint32_t* nb=KID+(size_t)c.id*KGRAPH;
        for(int e=0;e<KGRAPH;e++){ int nid=nb[e]; if(vis[nid])continue; vis[nid]=1;
            float d=(float)(qn+XN8[nid]-2*dot_i8(qc,CODES+(size_t)nid*DIM,DIM));
            if(rn<EF){ mheap_push(frontier,&fn,d,nid); res[rn++]=(DI){d,nid}; }
            else if(d<worst){ res[wi]=(DI){d,nid}; mheap_push(frontier,&fn,d,nid);
                worst=-1; for(int i=0;i<rn;i++) if(res[i].d>worst){worst=res[i].d;wi=i;} }
        }
    }
    // Move the KTOP smallest IDs in res into res[0..KTOP].
    for(int a2=0;a2<KTOP&&a2<rn;a2++){ int mi=a2; for(int b=a2+1;b<rn;b++) if(res[b].d<res[mi].d)mi=b; DI t=res[a2];res[a2]=res[mi];res[mi]=t; }
    return rn<KTOP?rn:KTOP;
}

static int env_i(const char* k,int d){ const char* v=getenv(k); return v?atoi(v):d; }

int main(void){
    DIM=env_i("BUILD_DIM",768);
    int threads=env_i("BUILD_THREADS",8);
    const char* sizes_s=getenv("BUILD_SIZES"); char sbuf[128]; strcpy(sbuf,sizes_s?sizes_s:"5000,10000,20000,50000");
    char brand[128]; size_t bl=sizeof brand; if(sysctlbyname("machdep.cpu.brand_string",brand,&bl,NULL,0))strcpy(brand,"?");
    printf("# SME int8 brute-force kNN-graph build  machine=%s  dim=%d K=%d threads=%d\n",brand,DIM,KGRAPH,threads);
    printf("%8s %12s %12s %12s %10s\n","N","build_ms","gemm_GMACs","recall@10","(search on flat exact-kNN graph)");

    int sizes[16],ns=0; for(char* tok=strtok(sbuf,",");tok&&ns<16;tok=strtok(NULL,",")) sizes[ns++]=atoi(tok);
    for(int si=0;si<ns;si++){
        N=sizes[si]; NPAD=((N+TILE-1)/TILE)*TILE;
        float* DBf=malloc((size_t)N*DIM*sizeof(float)); gen(DBf,(size_t)N*DIM,0xABCDEF);
        CODES=malloc((size_t)N*DIM); XN8=malloc((size_t)NPAD*sizeof(int32_t));
        KID=malloc((size_t)N*KGRAPH*sizeof(uint32_t)); KD=malloc((size_t)N*KGRAPH*sizeof(float));

        double t0=now_s();
        // Quantization and norms, matching HNSW PhaseB arena.
        for(int i=0;i<N;i++){ int64_t nn=0; for(int k=0;k<DIM;k++){int8_t v=q8(DBf[(size_t)i*DIM+k]); CODES[(size_t)i*DIM+k]=v; nn+=(int)v*(int)v;} XN8[i]=(int32_t)nn; }
        for(int j=N;j<NPAD;j++) XN8[j]=0;
        pack_db();
        // Multithreaded tiled GEMM and topK graph construction.
        pthread_t th[64]; BArg ar[64]; if(threads>64)threads=64;
        for(int t=0;t<threads;t++){ ar[t]=(BArg){t,threads}; pthread_create(&th[t],NULL,build_worker,&ar[t]); }
        for(int t=0;t<threads;t++) pthread_join(th[t],NULL);
        double build_ms=(now_s()-t0)*1e3;
        double gmacs=(double)N*N*DIM/(build_ms*1e-3)/1e9;

        // Recall: compare exact f32 top-10 with flat graph search for 200 new queries.
        float* Q=malloc((size_t)NQ*DIM*sizeof(float)); gen(Q,(size_t)NQ*DIM,0x123456);
        uint8_t* vis=malloc(N); DI* frontier=malloc((size_t)(N+16)*sizeof(DI)); DI* res=malloc((size_t)(EF+16)*sizeof(DI));
        int8_t* qc=malloc(DIM); int hit=0;
        for(int qi=0;qi<NQ;qi++){ const float* q=Q+(size_t)qi*DIM;
            // Exact top-10 through a full f32 scan.
            float gt_d[KTOP]; int gt_id[KTOP],gc=0;
            for(int j=0;j<N;j++){ float s=0; const float* x=DBf+(size_t)j*DIM; for(int k=0;k<DIM;k++){float d=q[k]-x[k];s+=d*d;}
                if(gc<KTOP){ gt_d[gc]=s;gt_id[gc]=j;gc++; }
                else { int wi=0; for(int a2=1;a2<KTOP;a2++) if(gt_d[a2]>gt_d[wi])wi=a2; if(s<gt_d[wi]){gt_d[wi]=s;gt_id[wi]=j;} }
            }
            // Graph search using SQ8.
            int32_t qn=0; for(int k=0;k<DIM;k++){int8_t v=q8(q[k]); qc[k]=v; qn+=(int)v*(int)v;}
            int got=search_flat(qc,qn,vis,frontier,res);
            for(int a2=0;a2<got;a2++){ for(int b=0;b<KTOP;b++) if(res[a2].id==gt_id[b]){hit++;break;} }
        }
        double recall=(double)hit/(NQ*KTOP);
        printf("%8d %12.1f %12.1f %12.4f\n",N,build_ms,gmacs,recall);

        free(DBf);free(CODES);free(XN8);free(KID);free(KD);free(Q);free(vis);free(frontier);free(res);free(qc);free(Bp8);
    }
    return 0;
}
