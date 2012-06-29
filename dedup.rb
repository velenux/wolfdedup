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

DataMapper::Logger.new(STDOUT, :debug) if $DEBUG

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
	@@read_chunk_size = 1024 * 256

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
	next if (not File.file?(f) or File.symlink?(f))
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

puts "Ignoring #{ignored_files} files (probably broken UTF-8 names)"

items = Set.new
FSItem.all.each do |item|
	items << item
end

# for each object
items.each do |item|
	# find other objects with the same size, but different inode and path
	same_size = FSItem.all(:size => item.size, :id.not => item.id, :inode.not => item.inode)
	# check these files against our current object
	same_size.each do |other|
		# if objects have identical md5+sha256
		if item == other
			puts "#{other.path} => #{item.path}" if $DEBUG
			# create a hardlink between them. FIXME: gracefully handle failed links
			FileUtils.ln other.path, item.path, :force => true
			# remove the other object from the set, so we won't check it again
			items.delete other
		end
	end
end

