



#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <x86intrin.h>
#define WIN "add $0x1000,%%r8\n add $0x1000,%%r9\n add $0x1000,%%r10\n add $0x1000,%%r11\n add $1,%%rax\n"
#define LOOPFN(W) static __attribute__((noinline)) void loop##W(long it) { \
  __asm__ volatile(".p2align 6\n1:\n.rept " #W "\n" WIN ".endr\n dec %%rdi\n jnz 1b\n" : "+D"(it) : : "rax","r8","r9","r10","r11","cc"); }
LOOPFN(32) LOOPFN(64) LOOPFN(96) LOOPFN(112) LOOPFN(128) LOOPFN(144) LOOPFN(160) LOOPFN(192) LOOPFN(224) LOOPFN(256) LOOPFN(320) LOOPFN(384)
typedef void (*lf)(long);
static const int WS[] = {32,64,96,112,128,144,160,192,224,256,320,384};
static const lf FS[] = {loop32,loop64,loop96,loop112,loop128,loop144,loop160,loop192,loop224,loop256,loop320,loop384};
#define NW 12
static volatile int go, stop;
static int cpuA = -1, cpuB = -1;
static void pin(int c) { cpu_set_t s; CPU_ZERO(&s); CPU_SET(c, &s); sched_setaffinity(0, sizeof s, &s); }
static long ITW = 20000000; 
static double run1(int k) { long it = ITW / WS[k]; FS[k](it / 20); unsigned long t0 = __rdtsc(); FS[k](it); unsigned long t1 = __rdtsc(); return (double)(t1 - t0) / ((double)it * WS[k]); }
static int bk = -1;
static void *sib(void *a) {
  pin(cpuB);
  while (!stop) { if (go && bk >= 0) FS[bk](2000); else __builtin_ia32_pause(); }
  return 0;
}
int main(int argc, char **argv) {
  cpu_set_t s; sched_getaffinity(0, sizeof s, &s);
  for (int c = 0; c < 1024 && cpuA < 0; c++) if (CPU_ISSET(c, &s)) {
    char p[128], b[64]; snprintf(p, sizeof p, "/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list", c);
    FILE *f = fopen(p, "r"); if (!f) continue; if (!fgets(b, sizeof b, f)) { fclose(f); continue; } fclose(f);
    int x, y; char sep; if (sscanf(b, "%d%c%d", &x, &sep, &y) == 3 && x == c && CPU_ISSET(y, &s)) { cpuA = x; cpuB = y; }
  }
  if (argc == 3) { cpuA = atoi(argv[1]); cpuB = atoi(argv[2]); }
  if (cpuA < 0) { fprintf(stderr, "no sibling pair (give two cpu numbers)\n"); return 1; }
  pin(cpuA);
  pthread_t th; pthread_create(&th, 0, sib, 0);
  printf("dsb pair=%d,%d ticks/window:", cpuA, cpuB);
  for (int k = 0; k < NW; k++) {
    bk = -1; go = 0; double a = 1e9, p = 1e9, q = 1e9;
    for (int r = 0; r < 3; r++) { double v = run1(k); if (v < a) a = v; }
    bk = k; go = 1; for (int r = 0; r < 3; r++) { double v = run1(k); if (v < p) p = v; } go = 0;
    bk = 0; go = 1; for (int r = 0; r < 3; r++) { double v = run1(k); if (v < q) q = v; } go = 0; bk = -1;
    printf(" W%d=%.2f/%.2f/%.2f", WS[k], a, p, q);
  }
  printf("\n"); stop = 1; pthread_join(th, 0); return 0;
}
