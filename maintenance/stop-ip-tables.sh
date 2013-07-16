for i in {1..11}
do
ssh root@machine$i 'service iptables stop'
done
