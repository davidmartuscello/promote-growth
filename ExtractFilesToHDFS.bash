

for f in *; do
  if [[ $f != *"meta"* ]]; then
    echo "File -> $f"
    a='./'
    b='FOLDER'
    p=$a$f$b
    echo "$p"
    7za e $f -o$p
    hdfs dfs -put $p /user/dm4350/project/allsites
    rm -r $p
  fi
done
