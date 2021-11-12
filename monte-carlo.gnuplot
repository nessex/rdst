set datafile separator "\t"
set style fill  transparent solid 0.35 noborder
set style circle radius 0.02
plot "< awk '{if($3 == \"ScanningSort\") print}' monte-carlo.tsv" u 1:2 t "ScanningSort" w p pt 7 ps 0.5, \
     "< awk '{if($3 == \"RecombinatingSort\") print}' monte-carlo.tsv" u 1:2 t "RecombinatingSort" w p pt 7 ps 0.5, \
     "< awk '{if($3 == \"ComparativeSort\") print}' monte-carlo.tsv" u 1:2 t "ComparativeSort" w p pt 7 ps 0.5, \
     "< awk '{if($3 == \"LsbSort\") print}' monte-carlo.tsv" u 1:2 t "LsbSort" w p pt 7 ps 0.5, \
     "< awk '{if($3 == \"RegionsSort\") print}' monte-carlo.tsv" u 1:2 t "RegionsSort" w p pt 7 ps 0.5, \
     "< awk '{if($3 == \"SkaSort\") print}' monte-carlo.tsv" u 1:2 t "SkaSort" w p pt 7 ps 0.5
pause mouse close
