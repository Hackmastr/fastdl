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
import traceback

global args
global jobs

# Terminal color codes
c_null	= "\x1b[00;00m"
c_red	= "\x1b[31;01m"
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

def RemoveEmptyFolders(path):
	if not os.path.isdir(path):
		return

	# remove empty subfolders
	files = os.listdir(path)
	if len(files):
		for f in files:
			fullpath = os.path.join(path, f)
			if os.path.isdir(fullpath):
				RemoveEmptyFolders(fullpath)

	# if folder empty, delete it
	files = os.listdir(path)
	if len(files) == 0:
		os.rmdir(path)

def Compress(item):
	sourcefile, destfile = item
	# Remove destination file if already exists
	if os.path.exists(destfile):
		os.remove(destfile)

	# Check whether directory tree exists at destination, create it if necessary
	directory = os.path.dirname(destfile)
	if not os.path.exists(directory):
		os.makedirs(directory)

	with open(sourcefile, "rb") as infile:
		with bz2.BZ2File(destfile, "wb", compresslevel=9) as outfile:
			shutil.copyfileobj(infile, outfile, 64*1024)

	if args.verbose:
		print("Compressed: {0}".format(sourcefile))

def Delete(item):
	item = item[0]
	if os.path.isdir(item):
		shutil.rmtree(item)
		if args.verbose:
			print("Deleted directory: {0}".format(item))
		return

	if os.path.exists(item):
		os.remove(item)
		if args.verbose:
			print("Deleted file: {0}".format(item))

	# Delte directory if empty
	directory = os.path.dirname(item)
	RemoveEmptyFolders(directory)

def Move(item):
	sourcepath, destpath = item

	if os.path.isdir(sourcepath):
		shutil.move(sourcepath, destpath)
		if args.verbose:
			print("Moved directory: {0} to {1}".format(os.path.basename(sourcepath), os.path.basename(destpath)))
		return

	# Check whether directory tree exists at destination, create it if necessary
	directory = os.path.dirname(destpath)
	if not os.path.exists(directory):
		os.makedirs(directory)

	shutil.move(sourcepath, destpath)

	if args.verbose:
		print("Moved file: {0} to {1}".format(os.path.basename(sourcepath), os.path.basename(destpath)))

def Worker():
	while True:
		job = jobs.get()
		try:
			job[0](job[1:])
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
		self.DestinationDirectory = os.path.abspath(destination)

		# Check whether directory exists
		if not os.path.isdir(self.SourceDirectory):
			print("Source path ({0}) doesn't exist!", self.SourceDirectory)
			sys.exit(1)

		# Check permission
		if not os.access(self.SourceDirectory, os.R_OK):
			print("Source path ({0}) is not readable! (Check permissions)", self.SourceDirectory)
			sys.exit(1)

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

	def Do(self): # Normal mode
		for dirpath, dirnames, filenames in os.walk(self.SourceDirectory):
			filenames.sort()
			for filename in [f for f in filenames if f.endswith(valid_extensions)]:
				self.Checkfile(dirpath, filename)

	def Checkfile(self, dirpath, filename):
		sourcefile = os.path.join(dirpath, filename)
		destfile = os.path.join(self.DestinationDirectory, os.path.relpath(dirpath, os.path.join(self.SourceDirectory, "..")), filename + ".bz2")

		exists = os.path.isfile(destfile)

		if args.verbose:
			if exists:
				status = "Exists"
				color = c_white
			else:
				status = "Added"
				color = c_green

			columns = int(os.popen("stty size", 'r').read().split()[1])
			rows = math.ceil((len(filename) + len(status))/columns)
			text = filename + ' '*(columns*rows - (len(filename) + len(status))) + color + status + c_null
			text += chr(8)*(len(text) + 1)
			print(text + '\n'*rows)

		if not exists:
			jobs.put((Compress, sourcefile, destfile))

def CheckfileReverse(dirpath, filename):
	destfile = os.path.join(dirpath, filename)
	exists = False
	for source in args.source:
		sourcefile = os.path.join(source, "..", os.path.relpath(dirpath, args.destination), filename[:-4]) # Remove last 4 characters -> ".bz2"
		if os.path.isfile(sourcefile):
			exists = True
			break

	if args.verbose:
		if exists:
			status = "Exists"
			color = c_white
		else:
			status = "Added"
			color = c_red

		columns = int(os.popen("stty size", 'r').read().split()[1])
		rows = math.ceil((len(filename) + len(status))/columns)
		text = filename + ' '*(columns*rows - (len(filename) + len(status))) + color + status + c_null
		text += chr(8)*(len(text) + 1)
		print(text + '\n'*rows)

	if not exists:
		jobs.put((Delete, destfile))

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Automate FastDL BZip2 process")
	parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose (debugging) output")
	parser.add_argument("-t", "--threads", type=int, default=1, help="Worker thread count")
	parser.add_argument("-r", "--reverse", action="store_true", help="Reverse mode. Walks through destination and checks if source exists. Deletes file if not found in source.")
	parser.add_argument("source", nargs='+', help="Source Path")
	parser.add_argument("destination", help="Destination Path")
	args = parser.parse_args()

	# Check whether directory exists
	if not os.path.isdir(args.destination):
		print("Destination path ({0}) doesn't exist!", args.destination)
		sys.exit(1)

	# Check permission
	if not os.access(args.destination, os.R_OK):
		print("Destination path ({0}) is not readable! (Check permissions)", args.destination)
		sys.exit(1)
	elif not os.access(args.destination, os.W_OK):
		print("Destination path ({0}) is not writeable! (Check permissions)", args.destination)
		sys.exit(1)

	jobs = queue.Queue()
	for i in range(args.threads):
		worker_thread = threading.Thread(target=Worker)
		worker_thread.daemon = True
		worker_thread.start()

	if args.reverse:
		for dirpath, dirnames, filenames in os.walk(args.destination):
			filenames.sort()
			for filename in [f for f in filenames if f.endswith(".bz2")]:
				CheckfileReverse(dirpath, filename)
		jobs.join()
		sys.exit(0)
	else:
		WatchManager = pyinotify.WatchManager()
		DirectoryHandlers = []
		for source in args.source:
			handler = DirectoryHandler(source, args.destination, WatchManager)
			DirectoryHandlers.append(handler)
			handler.Do()

	try:
		while True:
			for handler in DirectoryHandlers:
				handler.Loop()
	except KeyboardInterrupt:
		print("Waiting for remaining jobs to complete...")
		jobs.join()
		print("Exiting!")
