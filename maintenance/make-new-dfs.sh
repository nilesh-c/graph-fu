for i in {1..11}
do
ssh machine$i 'rm -rf /tmp/hadoop-hadoop'
done
