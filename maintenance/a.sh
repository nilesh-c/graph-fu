for i in {2..6}
do
#scp /home/hadoop/.ssh/authorized_keys  machine$i:/home/hadoop/.ssh/authorized_keys
#ssh machine$i "chown -R hadoop:hadoop ~hadoop && chmod 700 /home/hadoop/.ssh && chmod 600 /home/hadoop/.ssh/*"
scp /etc/hosts machine$i:/etc/
done
