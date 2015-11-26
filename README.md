# Status

README under construction

# Why

...

# Demo

...
```
./spit-server.rb s t tee o

./multiple-workers.sh -c echo -d foo -x 50

watch -d -n 1 './spit-client.rb show|mlr --oxtab cat'

grep stats o|mlr --oxtab put '$sec=$end-$start' then stats1 -a p0,p10,p50,p90,p100,co
sec_p0    0.042923
sec_p10   0.187047
sec_p50   0.353362
sec_p90   0.412769
sec_p100  0.664510
sec_count 1813
kerl@kerl-mbp[s0j0d1][spit]$ 

```

* percent-done autocompute
* inter-arrival autocompute
* write up ect
* cat msg-check-log.txt|grep mark-|mlr --opprint put '$pct=100*$ndone/($ntodo+$ndone);$goal=100' then cut -o -f pct,goal,time | estdonetime
* fage workdir
