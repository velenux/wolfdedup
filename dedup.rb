# encoding: utf-8

# WolfDedup: it solves problems.
# (a poor man's offline file-level deduplication solution)

# Copyright (C) Gilberto "Velenux" Ficara <g.ficara@stardata.it>
# Distributed under the terms of the GNU GPL v3 or later

require 'rubygems'
require 'logger'
require 'set'
require 'data_mapper'

# enable for debugging
$DEBUG = false

# enable for testing
$TEST  = false

DataMapper::Logger.new(STDOUT, :debug) if $TEST or $DEBUG

# CONFIG: your database parameters
DataMapper.setup(:default, 'mysql://username:password@hostname/dbname')

# CONFIG: if you need to specify your MySQL socket, comment the one above
# and use this one
#DataMapper.setup(:default, {
#    :adapter  => 'mysql',
#    :database => 'dedup',
#    :username => 'dedup',
#    :password => 'your_awesome_secure_password',
#    :host     => 'localhost',
#		:socket   => '/path/to/your/mysql.sock'
#})

# FileSystemItem
class FSItem
	include DataMapper::Resource
	@@read_chunk_size = 1024 * 256  # 256kb

	property :id,     Serial
	property :inode,  Integer, :min => 0, :max => 281474976710656
	property :md5,    String,  :length => 32
	property :sha256, String,  :length => 64
	property :path,   String,  :length => 4096
	property :size,   Integer, :min => 0, :max => 281474976710656
	property :owner,  Integer
	property :group,  Integer
	property :perms,  Integer
	property :run,    DateTime
	property :change, DateTime

	def md5
		calc_sums unless @md5
		@md5
	end

	def sha256
		calc_sums unless @sha256
		@sha256
	end

	def ==(item)
		self.md5 == item.md5 and self.sha256 == item.sha256
	end

	private
		def calc_sums
			require 'digest/md5'
			require 'digest/sha2'

			md5    = Digest::MD5.new()
			sha256 = Digest::SHA256.new()

			begin
				f = File.open(@path, 'r')

				while chunk = f.read(@@read_chunk_size)
					md5    << chunk
					sha256 << chunk
				end

				f.close

				self.md5    = md5.hexdigest
				self.sha256 = sha256.hexdigest
				self.save

			rescue => e
				@log.crit "Error calcolating checksums for \"#{@path}\" (#{e})!"
				raise e
			end

		end # calc_sums
end # FSItem

DataMapper.finalize

# choose migrate if you DON'T want to keep your data between runs
#DataMapper.auto_migrate!

# only upgrade database schema by default (keep data)
DataMapper.auto_upgrade!

now = Time.now
ignored_files = 0

# CONFIG (FIXME): specify the search paths here
# (in the future this will be rewritten to read paths from the command line)
Dir["/files1/**/*", "/files2/**/*", "/files3/**/*"].each do |f|

	# skip directories and other special files
	next if not File.file?(f)

	# skip symbolic links
	next if File.symlink?(f)

	# gather file statistics and create the object on database
	begin
		stats = File.stat(f)
		FSItem.create!(
			:path => f,
			:inode => stats.ino,
			:size => stats.size,
			:md5 => nil,
			:sha256 => nil,
			:owner => stats.uid,
			:group => stats.gid,
			:perms => stats.mode,
			:change => stats.mtime,
			:run => now
		)

	rescue => e
		ignored_files += 1
		STDERR.puts "\nError on #{f}\n> #{e}"
	end
end

# feedback on how many files have been ignored
puts "Ignoring #{ignored_files} files (probably broken UTF-8 names)"

# populate a set with all the FSItems we created, ordered by modification date
all_items_set = Set.new
FSItem.all(:order => [ :change.desc ]).each do |fs_item|
	all_items_set << fs_item
end

# for each object
all_items_set.each do |item|
	# iterate over other objects with the same size, but different inode and path
	# starting from the oldest and compare them  to the current one
	items_with_same_size = FSItem.all(:size => item.size, :id.not => item.id, :inode.not => item.inode, :order => [ :change.asc ]).each do |other|

		# if objects have identical md5+sha256
		if item == other

			# output the link that would have been created
			puts "#{other.path} => #{item.path}" if ($TEST or $DEBUG)

			# create a hardlink between them.
			if not $TEST
				begin
					FileUtils.ln other.path, item.path, :force => true
				
				rescue => e
					errors << "Cannot link #{other.path} to #{item.path}, #{e}"
				end

				# remove the other object from the set, so we won't check it again
				all_items_set.delete other
			end
		end
	end
end

