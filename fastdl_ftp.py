#!/usr/bin/python3
import argparse
import sys
import os
import math
import time
import threading
import queue
import bz2
import shutil
import pyinotify
import ftplib
import traceback
from io import BytesIO
from urllib.parse import urlparse

global args
global parsed
global commonprefix
global commonprefix_ftp
global jobs

USER = "****"
PASSWORD = "****"

# Terminal color codes
c_null	= "\x1b[00;00m"
c_red	= "\x1b[31;01m"
c_orange= "\x1b[33;01m"
c_green	= "\x1b[32;01m"
c_white	= "\x1b[37;01m"

# Valid file extensions to compress
valid_extensions = tuple([
	'bsp',					# maps
	'mdl', 'vtx', 'vvd',	# models
	'vtf', 'vmt', 'png',	# textures
	'wav', 'mp3',			# sounds
	'pcf',					# particles
	'ttf', 'otf',			# fonts
	'txt'					# misc
])

ignore_names = ["cs_assault.bsp", "cs_compound.bsp", "cs_havana.bsp",
				"cs_italy.bsp", "cs_militia.bsp", "cs_office.bsp",
				"de_aztec.bsp", "de_cbble.bsp", "de_chateau.bsp",
				"de_dust.bsp", "de_dust2.bsp", "de_inferno.bsp",
				"de_nuke.bsp", "de_piranesi.bsp", "de_port.bsp",
				"de_prodigy.bsp", "de_tides.bsp", "de_train.bsp",
				"test_hardware.bsp", "test_speakers.bsp"]

# inotify mask
NOTIFY_MASK = pyinotify.IN_CLOSE_WRITE | pyinotify.IN_DELETE | pyinotify.IN_MOVED_TO | pyinotify.IN_MOVED_FROM

def static_var(varname, value):
	def decorate(func):
		setattr(func, varname, value)
		return func
	return decorate

def PrettyPrint(filename, status):
	if status == "Exists":
		color = c_white
	elif status == "Added":
		color = c_orange
	elif status == "Done" or status == "Moved":
		color = c_green
	else:
		color = c_red

	columns = int(os.popen("stty size", 'r').read().split()[1])
	rows = math.ceil((len(filename) + len(status))/columns)
	text = filename + '.'*(columns*rows - (len(filename) + len(status))) + color + status + c_null
	text += chr(8)*(len(text) + 1)
	print(text + '\n'*rows)


# This is called a lot during startup.
@static_var("cache_path", 0)
@static_var("cache_resp", 0)
@static_var("cache_ftp", 0)
def FTP_FileExists(ftp, path):
	Exists = False
	try:
		# Cache should only be valid for one ftp connection
		if FTP_FileExists.cache_ftp != ftp:
			FTP_FileExists.cache_ftp = ftp
			FTP_FileExists.cache_path = 0
			FTP_FileExists.cache_resp = 0

		if FTP_FileExists.cache_path  != os.path.dirname(path):
			FTP_FileExists.cache_path = os.path.dirname(path)
			FTP_FileExists.cache_resp = []
			ftp.dir(os.path.dirname(path), FTP_FileExists.cache_resp.append)

		for line in FTP_FileExists.cache_resp:
			if line[0] == '-':
				line = line.split(maxsplit=8)[8]
				if line == os.path.basename(path):
					Exists = True
					break
	except ftplib.error_perm:
		pass

	return Exists

def FTP_DirExists(ftp, path):
	Exists = False
	try:
		resp = []
		ftp.dir(os.path.abspath(os.path.join(path, "..")), resp.append)
		for line in resp:
			if line[0] == 'd':
				line = line.split(maxsplit=8)[8]
				if line == os.path.basename(path):
					Exists = True
					break
	except ftplib.error_perm:
		pass

	return Exists

def Compress(ftp, item):
	sourcefile, destfile = item
	# Remove destination file if already exists
	if FTP_FileExists(ftp, destfile):
		ftp.delete(destfile)

	# Check whether directory tree exists at destination, create it if necessary
	directory = os.path.dirname(destfile)
	if not FTP_DirExists(ftp, directory):
		ftp.mkd(directory)

	tempfile = os.path.join("/tmp", os.path.basename(destfile))

	with open(sourcefile, "rb") as infile:
		with bz2.BZ2File(tempfile, "wb", compresslevel=9) as outfile:
			shutil.copyfileobj(infile, outfile, 64*1024)

	with open(tempfile, "rb") as temp:
		ftp.storbinary("STOR {0}".format(destfile), temp)

	os.remove(tempfile)

	PrettyPrint(os.path.relpath(sourcefile, commonprefix), "Done")

def Delete(ftp, item):
	item = item[0]

	try:
		ftp.delete(item)

		PrettyPrint(os.path.relpath(item, commonprefix_ftp), "Deleted")
	except ftplib.error_perm:
		pass

def Move(ftp, item):
	sourcepath, destpath = item

	# Check whether directory tree exists at destination, create it if necessary
	directory = os.path.dirname(destpath)
	if not FTP_DirExists(ftp, directory):
		ftp.mkd(directory)

	ftp.rename(sourcepath, destpath)

	PrettyPrint("{0} -> {1}".format(os.path.relpath(sourcepath, commonprefix_ftp), os.path.relpath(destpath, commonprefix_ftp)), "Moved")


def Worker():
	while True:
		job = jobs.get()
		try:
			if args.dry_run:
				print("Job: {0}({1})".format(job[0].__name__, job[1]))
			else:
				ftp = ftplib.FTP(parsed.netloc)
				ftp.login(USER, PASSWORD)

				job[0](ftp, job[1:])

				ftp.quit()
		except Exception as e:
			print("worker error {0}".format(e))
			print(traceback.format_exc())
		finally:
			jobs.task_done()


class EventHandler(pyinotify.ProcessEvent):
	def my_init(self, source, destination):
		self.SourceDirectory = os.path.abspath(source)
		self.DestinationDirectory = os.path.abspath(destination)

	def process_IN_CLOSE_WRITE(self, event):
		if not event.pathname.endswith(valid_extensions) or os.path.basename(event.pathname) in ignore_names:
			return

		destpath = os.path.join(self.DestinationDirectory, os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")))
		jobs.put((Compress, event.pathname, destpath + ".bz2"))

	def process_IN_DELETE(self, event):
		destpath = os.path.join(self.DestinationDirectory, os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")))
		if event.dir:
			if os.path.exists(destpath):
				jobs.put((Delete, destpath))
		else:
			if not event.pathname.endswith(valid_extensions) or os.path.basename(event.pathname) in ignore_names:
				return

			jobs.put((Delete, destpath + ".bz2"))

	def process_IN_MOVED_TO(self, event):
		# Moved from untracked directory, handle as new file
		if not hasattr(event, "src_pathname"):
			if not event.pathname.endswith(valid_extensions) or os.path.basename(event.pathname) in ignore_names:
				return

			destpath = os.path.join(self.DestinationDirectory, os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")))
			jobs.put((Compress, event.pathname, destpath + ".bz2"))
			return

		# Moved inside tracked directory, handle as rename
		sourcepath = os.path.join(self.DestinationDirectory, os.path.relpath(event.src_pathname, os.path.join(self.SourceDirectory, "..")))
		destpath = os.path.join(self.DestinationDirectory, os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")))

		if event.dir:
			jobs.put((Move, sourcepath, destpath))
		else:
			if event.src_pathname.endswith(valid_extensions) in ignore_names or os.path.basename(event.pathname) in ignore_names:
				return

			if not event.src_pathname.endswith(valid_extensions) and event.pathname.endswith(valid_extensions):
				# Renamed invalid_ext file to valid one -> compress
				jobs.put((Compress, event.pathname, destpath + ".bz2"))
				return

			elif event.src_pathname.endswith(valid_extensions) and not event.pathname.endswith(valid_extensions):
				# Renamed valid_ext file to invalid one -> delete from destination
				jobs.put((Delete, sourcepath + ".bz2"))
				return

			jobs.put((Move, sourcepath + ".bz2", destpath + ".bz2"))


class DirectoryHandler:
	def __init__(self, source, destination, watchmanager=None):
		self.SourceDirectory = os.path.abspath(source)
		self.DestinationDirectory = destination

		if watchmanager:
			self.WatchManager = watchmanager
			self.NotifyHandler = EventHandler(source=self.SourceDirectory, destination=self.DestinationDirectory)
			self.NotifyNotifier = pyinotify.Notifier(self.WatchManager, self.NotifyHandler, timeout=1000)
			self.NotifyWatch = self.WatchManager.add_watch(self.SourceDirectory, NOTIFY_MASK, rec=True, auto_add=True)

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.WatchManager.rm_watch(self.NotifyWatch, rec=True)

	def Loop(self):
		self.NotifyNotifier.process_events()
		while self.NotifyNotifier.check_events():
			self.NotifyNotifier.read_events()
			self.NotifyNotifier.process_events()

	def Do(self, ftp): # Normal mode
		for dirpath, dirnames, filenames in os.walk(self.SourceDirectory):
			filenames.sort()
			for filename in [f for f in filenames if f.endswith(valid_extensions) and f not in ignore_names]:
				self.Checkfile(ftp, dirpath, filename)

	def Checkfile(self, ftp, dirpath, filename):
		sourcefile = os.path.join(dirpath, filename)
		destfile = os.path.join(self.DestinationDirectory, os.path.relpath(dirpath, os.path.join(self.SourceDirectory, "..")), filename + ".bz2")

		if FTP_FileExists(ftp, destfile):
			PrettyPrint(os.path.relpath(sourcefile, commonprefix), "Exists")
		else:
			PrettyPrint(os.path.relpath(sourcefile, commonprefix), "Added")
			jobs.put((Compress, sourcefile, destfile))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Automate FastDL BZip2 process")
	parser.add_argument("-t", "--threads", type=int, default=1, help="Worker thread count")
	parser.add_argument("--dry-run", action="store_true", help="Test mode (don't run any jobs, just print them)")
	parser.add_argument("source", nargs='+', help="Source Path")
	parser.add_argument("destination", help="Destination Path")
	args = parser.parse_args()

	parsed = urlparse(args.destination)
	if not parsed.scheme == "ftp":
		print("Destination is not an ftp address!")
		sys.exit(1)

	ftp = ftplib.FTP(parsed.netloc)
	ftp.login(USER, PASSWORD)

	# make common prefix for better logging
	commonprefix = os.path.abspath(os.path.join(os.path.dirname(os.path.commonprefix(args.source)), ".."))
	commonprefix_ftp = os.path.dirname(parsed.path)

	jobs = queue.Queue()

	# Create initial jobs
	WatchManager = pyinotify.WatchManager()
	DirectoryHandlers = []
	for source in args.source:
		handler = DirectoryHandler(source, parsed.path, WatchManager)
		DirectoryHandlers.append(handler)
		handler.Do(ftp)

	ftp.quit()

	# Start worker threads
	for i in range(args.threads):
		worker_thread = threading.Thread(target=Worker)
		worker_thread.daemon = True
		worker_thread.start()

	# inotify loop
	try:
		while True:
			for handler in DirectoryHandlers:
				handler.Loop()
	except KeyboardInterrupt:
		print("Waiting for remaining jobs to complete...")
		jobs.join()
		print("Exiting!")
