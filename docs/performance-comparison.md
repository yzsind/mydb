# MyDB vs. MySQL performance

**November 17, 2026 · MyDB 1.0.0 vs. MySQL 8.0.46**

MyDB's strongest measured advantages are analytics and bulk loading:
**23.80x on TPC-H SF=10**, **48.34x on the matching TPC-DS SF=10 subset**,
and **10.04x on TPC-H SF=10 import**.

## Analytics

Lower time is better. Speedup = MySQL time / MyDB time.

| Workload | Scope | MyDB | MySQL | Speedup |
| --- | ---: | ---: | ---: | ---: |
| TPC-H query medians | SF=1, 22/22 matching | 1.977 s | 28.748 s | **14.54x** |
| TPC-H query medians | SF=10, 22/22 matching | 50.636 s | 1,205.299 s | **23.80x** |
| TPC-DS matching files | SF=1, 92/99 matching | 13.379 s | 489.150 s | **36.56x** |
| TPC-DS matching files | SF=10, 84/99 matching | 92.427 s | 4,468.101 s | **48.34x** |

MyDB was faster on **all 22 TPC-H queries at both scales**, and on **82 of 84
matching TPC-DS files at SF=10**. TPC-H results matched on all 22 queries.
MyDB successfully executed all 99 TPC-DS files at both scales.

TPC-DS totals cover matching subsets, not the full suite. MySQL completed 92/99
files at SF=1 and 86/99 at SF=10; the remaining files failed or timed out.
SF=10 Q30 and Q81 returned different normalized results and are excluded from
speedups. These two-engine subsets differ from the historical three-engine scope.

## Transactions and loading

Higher throughput is better; ratios below use MyDB / MySQL throughput.
sysbench uses 100M rows and 20 clients; TPC-C uses 10 warehouses and 20 terminals.

| Workload | MyDB | MySQL | Throughput ratio |
| --- | ---: | ---: | ---: |
| Point select | 295,724.25 queries/s | 184,108.16 queries/s | **1.61x** |
| Read only | 13,521.93 transactions/s | 7,477.23 transactions/s | **1.81x** |
| Read write | 4,767.80 transactions/s | 4,615.12 transactions/s | **1.03x** |
| TPC-C-derived New-Order | 127,314.40 committed/min | 110,524.80 committed/min | **1.15x** |

Read-only P95 latency was **2.52 ms vs. 4.65 ms**; New-Order P95 was
**9 ms vs. 12 ms**. sysbench reported zero errors or reconnects. TPC-C recorded
zero transaction errors, and both engines passed its eight business consistency checks.

| Loading workload | MyDB | MySQL | Speedup |
| --- | ---: | ---: | ---: |
| TPC-H SF=10 import | 37.281 s | 374.490 s | **10.04x** |
| sysbench prepare, 100M rows | 84.147 s | 143.122 s | **1.70x** |

Loading ratios use MySQL time / MyDB time. TPC-H import includes upload and
durable commit, excluding subsequent secondary-index creation and ANALYZE;
sysbench prepare includes secondary-index creation.

## Per-query comparison

Times are seconds; lower is better. Ratios use unrounded measurements.
TPC-H reports the median of three timed runs after one warmup; TPC-DS reports
**one execution per file, with no query warmup**. Matching compares normalized
results with numbers rounded to four decimal places. `Error` means execution
failed; `Timeout` means the 300-second limit was reached. No speedup is assigned
to failed, timed-out or different-result files. A ratio below 1x favors MySQL.

<details>
<summary>TPC-H — all 22 queries, SF=1 and SF=10</summary>

| Query | SF=1 MyDB (s) | SF=1 MySQL (s) | Speedup | SF=10 MyDB (s) | SF=10 MySQL (s) | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 0.136 | 8.770 | **64.47x** | 2.234 | 113.443 | **50.78x** |
| Q2 | 0.038 | 0.087 | **2.26x** | 0.147 | 0.892 | **6.07x** |
| Q3 | 0.094 | 1.337 | **14.28x** | 2.249 | 80.138 | **35.63x** |
| Q4 | 0.052 | 0.301 | **5.77x** | 3.851 | 18.370 | **4.77x** |
| Q5 | 0.102 | 0.855 | **8.35x** | 2.593 | 12.841 | **4.95x** |
| Q6 | 0.064 | 1.575 | **24.76x** | 0.776 | 31.192 | **40.20x** |
| Q7 | 0.118 | 0.876 | **7.42x** | 3.130 | 273.346 | **87.34x** |
| Q8 | 0.100 | 1.872 | **18.77x** | 3.574 | 66.592 | **18.63x** |
| Q9 | 0.214 | 1.835 | **8.59x** | 5.703 | 226.469 | **39.71x** |
| Q10 | 0.145 | 1.169 | **8.07x** | 8.842 | 33.090 | **3.74x** |
| Q11 | 0.034 | 0.119 | **3.48x** | 0.252 | 2.275 | **9.02x** |
| Q12 | 0.109 | 2.181 | **20.08x** | 4.678 | 42.707 | **9.13x** |
| Q13 | 0.070 | 2.810 | **40.23x** | 0.843 | 41.221 | **48.88x** |
| Q14 | 0.072 | 0.251 | **3.47x** | 1.908 | 12.159 | **6.37x** |
| Q15 | 0.058 | 0.507 | **8.75x** | 0.686 | 24.123 | **35.17x** |
| Q16 | 0.129 | 0.315 | **2.43x** | 0.479 | 2.647 | **5.52x** |
| Q17 | 0.010 | 0.244 | **24.11x** | 0.150 | 2.833 | **18.88x** |
| Q18 | 0.079 | 2.068 | **26.34x** | 0.911 | 71.889 | **78.95x** |
| Q19 | 0.017 | 0.101 | **6.01x** | 0.188 | 1.406 | **7.46x** |
| Q20 | 0.101 | 0.208 | **2.07x** | 3.129 | 30.081 | **9.61x** |
| Q21 | 0.219 | 1.131 | **5.16x** | 4.180 | 116.174 | **27.79x** |
| Q22 | 0.017 | 0.137 | **8.00x** | 0.133 | 1.412 | **10.65x** |

</details>

<details>
<summary>TPC-DS — all 99 query files, SF=1 and SF=10</summary>

| Query | SF=1 MyDB (s) | SF=1 MySQL (s) | Speedup | SF=10 MyDB (s) | SF=10 MySQL (s) | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 0.054 | 41.755 | **778.95x** | 0.294 | Timeout | — |
| Q2 | 0.149 | Error | — | 1.504 | Error | — |
| Q3 | 0.020 | 0.014 | 0.72x | 0.143 | 0.053 | 0.37x |
| Q4 | 0.862 | 95.353 | **110.59x** | 8.697 | Timeout | — |
| Q5 | 0.069 | 14.147 | **205.49x** | 0.954 | 188.417 | **197.50x** |
| Q6 | 0.315 | 64.266 | **203.82x** | 3.274 | Timeout | — |
| Q7 | 0.063 | 3.698 | **58.29x** | 0.997 | 133.265 | **133.61x** |
| Q8 | 0.047 | 0.366 | **7.73x** | 0.096 | Timeout | — |
| Q9 | 0.128 | 5.493 | **42.79x** | 1.136 | 61.319 | **53.99x** |
| Q10 | 0.078 | 0.096 | **1.22x** | 0.626 | 18.833 | **30.07x** |
| Q11 | 0.484 | 58.842 | **121.50x** | 4.904 | Timeout | — |
| Q12 | 0.039 | 0.049 | **1.25x** | 0.245 | 3.860 | **15.77x** |
| Q13 | 0.116 | 1.682 | **14.48x** | 0.902 | 96.833 | **107.40x** |
| Q14 | 1.694 | Error | — | 13.852 | Error | — |
| Q15 | 0.067 | 0.376 | **5.65x** | 0.344 | 6.627 | **19.25x** |
| Q16 | 0.042 | 1.077 | **25.70x** | 0.213 | 16.601 | **78.01x** |
| Q17 | 0.162 | 0.642 | **3.97x** | 1.589 | 120.731 | **75.99x** |
| Q18 | 0.126 | 0.899 | **7.16x** | 0.728 | 22.236 | **30.56x** |
| Q19 | 0.058 | 0.572 | **9.84x** | 0.540 | 4.669 | **8.64x** |
| Q20 | 0.046 | 0.086 | **1.86x** | 0.227 | 5.629 | **24.78x** |
| Q21 | 0.144 | 0.280 | **1.94x** | 1.799 | 4.245 | **2.36x** |
| Q22 | 0.158 | 6.014 | **38.18x** | 1.597 | 99.738 | **62.47x** |
| Q23 | 0.628 | Error | — | 8.586 | Error | — |
| Q24 | 0.115 | 0.066 | 0.57x | 0.724 | 35.055 | **48.39x** |
| Q25 | 0.108 | 0.417 | **3.85x** | 0.443 | 6.695 | **15.13x** |
| Q26 | 0.047 | 5.274 | **112.24x** | 0.277 | 79.607 | **287.78x** |
| Q27 | 0.083 | 3.247 | **39.08x** | 0.665 | 56.359 | **84.78x** |
| Q28 | 0.205 | 4.528 | **22.10x** | 2.058 | 45.020 | **21.87x** |
| Q29 | 0.128 | 0.126 | 0.98x | 0.516 | 6.929 | **13.44x** |
| Q30 | 0.040 | 0.104 | **2.59x** | 0.178 | 2.539 | Mismatch |
| Q31 | 0.080 | 27.091 | **339.80x** | 0.670 | Timeout | — |
| Q32 | 0.020 | 0.027 | **1.32x** | 0.218 | 4.719 | **21.62x** |
| Q33 | 0.060 | 0.213 | **3.55x** | 0.564 | 10.078 | **17.85x** |
| Q34 | 0.062 | 1.677 | **27.27x** | 0.358 | 55.320 | **154.61x** |
| Q35 | 0.134 | 12.147 | **90.50x** | 1.150 | 92.651 | **80.57x** |
| Q36 | 0.124 | 3.067 | **24.81x** | 0.601 | 69.201 | **115.16x** |
| Q37 | 0.016 | 0.006 | 0.39x | 0.035 | 0.078 | **2.21x** |
| Q38 | 0.222 | 2.776 | **12.50x** | 2.101 | 110.921 | **52.80x** |
| Q39 | 0.358 | 4.385 | **12.26x** | 3.792 | 77.715 | **20.50x** |
| Q40 | 0.042 | 0.120 | **2.86x** | 0.209 | 60.179 | **288.06x** |
| Q41 | 0.012 | 1.306 | **106.95x** | 0.030 | 63.350 | **2136.22x** |
| Q42 | 0.029 | 0.033 | **1.12x** | 0.330 | 10.949 | **33.13x** |
| Q43 | 0.072 | 11.546 | **160.18x** | 0.425 | 60.013 | **141.16x** |
| Q44 | 0.199 | 1.612 | **8.10x** | 0.719 | 4.199 | **5.84x** |
| Q45 | 0.086 | 0.119 | **1.38x** | 0.669 | 7.416 | **11.08x** |
| Q46 | 0.122 | 2.056 | **16.82x** | 0.582 | 37.476 | **64.34x** |
| Q47 | 0.560 | 15.954 | **28.47x** | 1.856 | 191.808 | **103.37x** |
| Q48 | 0.148 | 1.525 | **10.29x** | 0.967 | 85.048 | **87.95x** |
| Q49 | 0.090 | Error | — | 1.054 | Error | — |
| Q50 | 0.034 | 0.094 | **2.76x** | 0.512 | 2.907 | **5.68x** |
| Q51 | 1.016 | Error | — | 11.878 | Error | — |
| Q52 | 0.030 | 0.054 | **1.78x** | 0.378 | 6.270 | **16.58x** |
| Q53 | 0.070 | 0.618 | **8.80x** | 0.403 | 110.892 | **275.43x** |
| Q54 | 0.057 | Error | — | 0.266 | Error | — |
| Q55 | 0.029 | 0.051 | **1.74x** | 0.375 | 6.206 | **16.56x** |
| Q56 | 0.045 | 0.562 | **12.37x** | 0.525 | 8.964 | **17.08x** |
| Q57 | 0.257 | 9.714 | **37.78x** | 1.396 | 109.784 | **78.67x** |
| Q58 | 0.099 | 0.130 | **1.31x** | 0.594 | 1.636 | **2.75x** |
| Q59 | 0.146 | 6.049 | **41.38x** | 1.157 | 64.857 | **56.07x** |
| Q60 | 0.059 | 0.832 | **14.22x** | 0.516 | 11.650 | **22.57x** |
| Q61 | 0.052 | 0.008 | 0.15x | 0.034 | 0.008 | 0.24x |
| Q62 | 0.035 | 0.711 | **20.24x** | 0.210 | 13.236 | **63.01x** |
| Q63 | 0.063 | 0.271 | **4.32x** | 0.375 | 2.869 | **7.65x** |
| Q64 | 2.015 | 0.696 | 0.35x | 3.600 | 18.107 | **5.03x** |
| Q65 | 0.086 | 2.837 | **32.89x** | 1.994 | 94.813 | **47.54x** |
| Q66 | 0.126 | 1.588 | **12.60x** | 0.520 | 83.486 | **160.61x** |
| Q67 | 0.391 | 6.505 | **16.64x** | 16.207 | 187.576 | **11.57x** |
| Q68 | 0.083 | 0.539 | **6.49x** | 0.414 | 32.037 | **77.39x** |
| Q69 | 0.069 | 0.225 | **3.24x** | 0.554 | 42.904 | **77.48x** |
| Q70 | 0.095 | 3.643 | **38.17x** | 0.755 | 194.396 | **257.41x** |
| Q71 | 0.064 | 0.525 | **8.15x** | 0.839 | 17.726 | **21.12x** |
| Q72 | 0.311 | 1.476 | **4.75x** | 4.691 | 43.252 | **9.22x** |
| Q73 | 0.041 | 0.488 | **11.76x** | 0.356 | 50.540 | **141.99x** |
| Q74 | 0.342 | 9.240 | **26.99x** | 3.459 | 215.965 | **62.44x** |
| Q75 | 0.286 | 2.966 | **10.38x** | 4.563 | 40.137 | **8.80x** |
| Q76 | 0.081 | 0.923 | **11.43x** | 1.407 | 19.671 | **13.98x** |
| Q77 | 0.069 | 0.327 | **4.76x** | 0.688 | 16.349 | **23.77x** |
| Q78 | 0.693 | 23.084 | **33.31x** | 3.766 | 289.549 | **76.89x** |
| Q79 | 0.093 | 3.974 | **42.58x** | 0.632 | 131.395 | **207.92x** |
| Q80 | 0.121 | 0.547 | **4.52x** | 0.684 | 11.294 | **16.52x** |
| Q81 | 0.044 | 0.172 | **3.91x** | 0.272 | 4.754 | Mismatch |
| Q82 | 0.038 | 0.022 | 0.58x | 0.096 | 2.285 | **23.78x** |
| Q83 | 0.099 | 0.038 | 0.39x | 0.361 | 1.628 | **4.51x** |
| Q84 | 0.038 | 0.012 | 0.32x | 0.106 | 0.204 | **1.92x** |
| Q85 | 0.231 | 0.184 | 0.79x | 0.527 | 5.341 | **10.13x** |
| Q86 | 0.037 | 0.609 | **16.39x** | 0.297 | 16.320 | **54.90x** |
| Q87 | 0.255 | 2.758 | **10.82x** | 2.442 | 107.301 | **43.94x** |
| Q88 | 0.094 | 1.762 | **18.74x** | 2.564 | 145.942 | **56.91x** |
| Q89 | 0.085 | 0.824 | **9.72x** | 0.492 | 112.407 | **228.45x** |
| Q90 | 0.035 | 1.508 | **43.41x** | 0.206 | 17.739 | **86.32x** |
| Q91 | 0.055 | 0.018 | 0.32x | 0.158 | 2.246 | **14.20x** |
| Q92 | 0.018 | 0.018 | 0.97x | 0.085 | 0.144 | **1.69x** |
| Q93 | 0.089 | 0.046 | 0.51x | 0.475 | 1.690 | **3.55x** |
| Q94 | 0.038 | 0.418 | **10.90x** | 0.338 | 6.968 | **20.62x** |
| Q95 | 0.040 | 4.689 | **118.59x** | 0.239 | 162.481 | **678.79x** |
| Q96 | 0.033 | 0.122 | **3.65x** | 0.350 | 17.871 | **51.06x** |
| Q97 | 0.179 | Error | — | 2.227 | Error | — |
| Q98 | 0.063 | 0.297 | **4.68x** | 0.423 | 3.610 | **8.54x** |
| Q99 | 0.046 | 1.850 | **40.28x** | 0.335 | 81.604 | **243.32x** |

</details>

## Test conditions

Same 16-vCPU Xeon Platinum 8369B host, 10 GiB database memory limits with no swap,
8 GiB MyDB memory target / MySQL buffer pool, READ COMMITTED and synchronous
durable commits. MySQL-format binlogging was disabled. Analytical MyDB runs
retained automatic zone maps and supported up to 16-way parallelism.

OLTP tables use full 600-second averages, after 60 seconds of sysbench warmup
or 120 seconds of TPC-C warmup. New-Order counts exclude expected rollbacks and
post-window drain. Cache states and memory mechanisms are not fully equivalent.
These are workload-specific observations without confidence intervals;
TPC-derived measurements are not official or TPC-audited results.
