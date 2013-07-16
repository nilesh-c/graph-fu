from os import system

for i in open("nodes"):
#	system("ping %s" % i)
	if "13.34" not in i:
		print "Doing %s" % i
#		system("scp /etc/hosts root@" + i.strip() + ":/etc/")
#		system("scp ~hadoop/.ssh/id_rsa.pub root@%s:~hadoop/.ssh/abc" % i.strip())
#		system("ssh root@%s \"cat ~hadoop/.ssh/abc > ~hadoop/.ssh/authorized_keys && cat ~hadoop/.ssh/id_rsa.pub >> ~hadoop/.ssh/authorized_keys && chown -R hadoop:hadoop ~hadoop/.ssh && chmod 600 ~hadoop/.ssh/* && rm ~hadoop/.ssh/abc\"" % i.strip())
		system("ssh root@%s service iptables stop" % i.strip())
