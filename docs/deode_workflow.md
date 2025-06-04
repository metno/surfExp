
Example mars request

```
retrieve,
    TARGET     = "/scratch/sbu/dt_20240916_20240916.grib2",
    DATE       = 20240916,
    TIME       = 0,
    STEP       = 0/1/2/3,
    PARAM      = 129/134/144/165/166/167/168/169/175/228,
    EXPVER     = iekm,
    CLASS      = RD,
    LEVTYPE    = SFC,
    TYPE       = AN/FC,
    STREAM     = OPER,
    EXPECT     = ANY,
    GRID       = 0.04/0.04,
    AREA       = 58.0/8.0/62.0/12.0
```
