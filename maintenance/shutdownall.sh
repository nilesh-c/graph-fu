for i in {2..11}
do
ssh machine$i "shutdown -h now"
done
