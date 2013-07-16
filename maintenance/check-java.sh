for i in {1..11}
do
ssh machine$i 'ps aux | grep java | wc -l'
done
