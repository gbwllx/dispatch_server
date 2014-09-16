#!/usr/bin/env python
#coding=utf-8

import pexpect
import pxssh
import sys
import time
import os
import getpass
import pdb
import random
import threading
from optparse import OptionParser
from optparse import OptionGroup

MSG=False

def ssh_cmd(username, hostname, cmd, passwd):
	command = 'ssh %s@%s "%s"' %(username, hostname, cmd)
	try:
		child = pexpect.spawn(command)
		index = child.expect(['password', pexpect.TIMEOUT, pexpect.EOF])
		if index == 0:
			child.sendline(passwd)
			r = child.read()
			child.expect(pexpect.EOF)
		elif index == 1:
			ret = str(child)
			child.close()
			return 0, 'timeout error %s' %ret
		else:
			ret = str(child)
			child.close()
			return 0, 'eof error %s' %ret
		
	except:
		ret = str(child)
		child.close()
		return 0, 'I do not know the errortype now %s' %ret
	else:
		child.close()
		return 1, r
	
def data_slice(slicenum, filepath):
	filedir, filename = os.path.split(filepath)
	if os.path.basename(filedir) != 'tmp':
		tmpdir = '%s/tmp' %filedir
		if not os.path.isdir(tmpdir):	
			os.makedirs(tmpdir)
		filedir = tmpdir
	namecnt = 0
	filelist = []
	namepre = filename.split('.')[0]
	suffix = filename.split('.')[1]
	with open(filepath, 'r') as f:
		lines = f.readlines()
		maxcnt = len(lines) / slicenum + 1
		while namecnt < slicenum:
			wlines = lines[namecnt*maxcnt+0:namecnt*maxcnt+maxcnt]
			fname = '%s/%s_%d.%s' %(filedir, namepre, namecnt, suffix)
			filelist.append(fname)
			with open(fname, 'w') as f2:
				f2.writelines(wlines)
				f2.close()
			namecnt += 1
		f.close()	
	
	return filelist
		
	
def file_upload(file, username, hostname, todir, passwd):		
	command = 'scp %s %s@%s:%s' %(file, username, hostname, todir)
	
	try:
		child = pexpect.spawn(command)
		index = child.expect(['password', pexpect.TIMEOUT, pexpect.EOF])
		if index == 0:
			child.sendline(passwd)
			child.expect(pexpect.EOF)
		elif index == 1:
			ret = str(child)
			child.close()
			return 0, 'timeout error %s' %ret
		else:
			ret = str(child)
			child.close()
			return 0, 'eof error %s' %ret
	except:
		ret = str(child)
		child.close()
		return 0, 'I do not know the errortype now %s' %ret
	else:
		r = child.read()
		child.close()
		return 1, r
				
def dir_upload(username, hostname, todir, passwd):
	dir = '/home/%s/dispatch/upload' %username
	
	command = 'scp -r %s %s@%s:%s' %(dir, username, hostname, todir)
	
	try:
		child = pexpect.spawn(command)
		child.expect('password: ')
		child.sendline(passwd)
		child.expect(pexpect.EOF)
	except:
		ret = str(child)
		child.close()
		return 0, 'I do not know the errortype now %s' %ret
	else:
		child.close()
		return 1, 'no error'
		
def dir_download(username, hostname, todir, passwd):	
	dir = '/home/%s/data_check'  %username
	todir = '/home/%s/data_check/result' %username	
	command = 'scp -r %s@%s:%s %s' %(username, hostname, todir, dir)
	
	try:
		child = pexpect.spawn(command)
		child.expect('password: ')
		child.sendline(passwd)
		child.expect(pexpect.EOF)
	except:
		ret = str(child)
		child.close()
		return 0, 'I do not know the errortype now %s' %ret
	else:
		child.close()
		return 1, 'no error'

def mk_todir(username, hostname, passwd, dir, worker):
	todir = '%s/data/tmp' %(dir)
	cmd = 'if [ -f \'%s\' ];then rm -f %s;fi;if [ ! -d \'%s\' ];then mkdir -p %s; fi' %(todir, todir, todir, todir)
	errcode, errmsg = ssh_cmd(username, hostname, cmd, passwd)
	
	return errcode, errmsg, todir
	
def get_passwd(username, hostname):
	try:
		s = pxssh.pxssh()
		passwd = getpass.getpass()
		passwd = passwd + '\n'
		s.login(hostname, username, passwd)
	except pxssh.ExceptionPxssh as e:
		print(e)
		passwd = get_passwd(username, hostname)
		return passwd	
	else:
		s.logout()
		return passwd

def machine_stat_det(username, hostname, passwd, cmd):
	type, cpu = ssh_cmd(username, hostname, cmd, passwd)
	if type == 0:
		return 0
	
	print hostname
	print cpu
	print len(cpu)-1
	
def machines_stat_det(username, machinelist, passwd, cmdnum):
	if cmdnum == "status":
		cmd = 'ps -ef | grep data_check_main | grep -v grep'
	elif cmdnum == "kill":
		cmd = "ps -ef | grep data_check_main | grep -v grep | awk '{print $2}' | xargs kill -9"
	elif cmdnum == "clresult":
		cmd = 'rm -rf /home/%s/data_check/result/*' %username
	else:
		cmd = cmdnum
	
	print cmd
	for i in range(0, len(machinelist)):
		hostname = machinelist[i].strip()
		machine_stat_det(username, hostname, passwd, cmd)


def write_to_file(freelist, busylist, errorlist,maclistfile):
	with open(maclistfile, 'w') as f:
		f.writelines(freelist)
		f.close()
		
def read_file(maclistfile):
	with open(maclistfile, 'r') as f:
		lines = f.readlines()
		f.close()
	return lines

def work(username, hostname, passwd, file):
	cmd = 'xxx/python /home/%s/upload/xx.py %s' %(username, file)
	command = 'ssh %s@%s "%s"' %(username, hostname, cmd)
	try:
		child = pexpect.spawn(command)
		child.expect('password')
		child.sendline(passwd)
		print command
		while True:
			index = child.expect([pexpect.EOF, pexpect.TIMEOUT])
			if index == 0:
				break
			if index == 1:
				pass
	except pexpect.EOF:
		print hostname
		print("Exception was thrown")
		print("debug information:")
		print(str(child))
	except:
		print hostname
		print("Exception was thrown")
		print("debug information:")
		print(str(child))
	else:
		child.close()
		print hostname
		
def run():
	usage = "dispatch_server.py [-u][-f][-d][-p][-U]|[-c]|[-s]"
	optParser = OptionParser(usage)
	group = OptionGroup(optParser,"Auxiliary Funciton Options")
	group.add_option("-c","--command",action = "store",type="str",dest = "cmd",
						help='''status for process status
		 					kill for kill process 
							clresult for clear result directory 
							other string for you can input cmd yourself''')
	group.add_option("-s","--slice",action = "store",type="str",dest = "slicefile",nargs=2, help="file to slice")
	optParser.add_option_group(group)
	optParser.add_option("-d","--download",action = "store_true",dest = "downdir",help="input which directory you want to download")
	optParser.add_option("-f","--file",action = "store",type="str",dest = "filename", help="file to check")
	optParser.add_option("-p","--print",action = "store_true",dest = "outputinfo", help="if on, output the process infomation")
	optParser.add_option("-u","--upload",action = "store_false",dest = "uploaddir", help="if update")
	optParser.add_option("-U","--user",action = "store",type="str",dest = "username", default="ffff", help="username")
	options, args = optParser.parse_args()
	
	#pdb.set_trace()
	username = options.username
	dir = '/home/%s' %username
	maclistfile ='%s/dispatch/ffff_maclistfile.txt' %dir
	freelist = read_file(maclistfile)
	hostname = freelist[0]
	busylist = []	
	
	if options.slicefile == None:	
		passwd = get_passwd(username, hostname)
	
	'''update executable program'''
	if options.uploaddir != None:
		for i in range(0, len(freelist)):
			hostname = freelist[i].strip()
			errcode, errmsg = dir_upload(username, hostname, dir, passwd)
			if errcode == 0:
				print errmsg
				break
			print '%d.%s: %s' %(i, hostname, errmsg)
	
		print '%d host upload done' %(len(freelist))
	
	'''dispatch file and data check'''
	if options.filename != None:
		if(len(freelist)) > 20:
			worker = 20
		else:
			worker = len(freelist)
		filelist = data_slice(worker, options.filename)
		work_threads = []	


		for i in range(0, worker):
			hostname = freelist[i].strip()
		
			errcode, errmsg, todir = mk_todir(username, hostname, passwd, dir, worker)
			if errcode == 0:
				print errmsg
				break
			errcode, errmsg = file_upload(filelist[i], username, hostname, todir, passwd)
			if errcode == 0:
				print errmsg
				break
			
			work_thread = threading.Thread(target = work, args = (username, hostname, passwd, filelist[i]))
			work_threads.append(work_thread)
			print work_thread, hostname
			work_thread.start()
	
		for job in work_threads:
			print str(job)+' waiting!'
			job.join()
	
	#time.sleep(10)
	'''download result'''
	if options.downdir != None:
		for i in range(0, len(freelist)):
			hostname = freelist[i].strip()
			errcode, errmsg = dir_download(username, hostname, dir, passwd)
			if errcode == 0:
				print errmsg
				break
			print '%d.%s: %s' %(i, hostname, errmsg)
	
		print '%d host download done' %(len(freelist))
	
	'''file slicing'''
	if options.slicefile != None:
		filelist = data_slice(int(options.slicefile[1]), options.slicefile[0])
		
	'''check the progress status or other command'''
	if options.cmd != None: 
		machines_stat_det(username, freelist, passwd, options.cmd)		
		
if __name__ == '__main__':
	run()
	print 'end'
