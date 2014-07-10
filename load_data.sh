folder='/Users/mthangavelu/Documents/development/lookout/datasampler/data/'

find $folder -name "*.csv" -print | while read f; do
    echo $f
    mysqlimport --local --fields-enclosed-by="\"" --fields-terminated-by=, --lines-terminated-by="\n"  -h localhost --user=root -p cdc_test $f
done

