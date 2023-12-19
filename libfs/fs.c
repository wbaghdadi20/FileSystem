#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "disk.h"
#include "fs.h"

#define FAT_EOC 0xFFFF
#define SIG_BYTE_LEN 8 // signature byte length
#define RD_PADDING_LEN 10 // root directory padding
#define SB_PADDING_LEN 4079 // superblock padding
#define MAX_DATA_BLOCKS 8192
#define UNUSED -1 // set unused indexes to -1

/* TODO: Phase 1 */

// check if disk is opened or not
int disk_open = 0;

// keep track of free entries
int root_dir_frees = FS_FILE_MAX_COUNT;
int is_mounted = 0;
uint16_t fat_frees;
int MAX_FAT_READ_SIZE;

/* to keep track of fds */
typedef struct FDEntry {
	// root directory index
	int fd_num;
	// offset
	uint64_t offset;
} FDEntry;

// array that holds files
// fd fd_table with info about fd #, offset, size, filename ...
FDEntry fd_table[FS_OPEN_MAX_COUNT];
int open_files;

// Superblock
typedef struct __attribute__((packed)){
	uint8_t signature [SIG_BYTE_LEN];
	uint16_t block_amt;
	uint16_t rd_start_idx;
	uint16_t data_start_idx;
	uint16_t data_block_amt;
	uint8_t fat_block_amt;
	uint8_t padding [SB_PADDING_LEN];
} Superblock;

// entry (for root directory)
typedef struct __attribute__((packed)) {
	char name [FS_FILENAME_LEN];
	uint32_t size;
	uint16_t first_db_idx;
	uint8_t padding [RD_PADDING_LEN];
} RootDirEntry;

// superblock
Superblock sblock;
uint16_t *fat_array;
RootDirEntry root_dir [FS_FILE_MAX_COUNT];

int min(int n1, int n2){
	return (n1 < n2)? n1: n2;
}

// ------------ File helper functions ------------------

// returns 1 if file with name 'filename' already exists, 0 otherwise
int file_exists (const char *filename) {
	for (int i = 0; i < FS_FILE_MAX_COUNT; i++) {
		// check if filename already exists in root dir
		if (!strcmp(root_dir[i].name, filename)) {
			return 1;
		}
	}

	return 0;
}

// returns 1 if 0 <= fd < 32 , 0 otherwise
int valid_fd (int fd) {
	return (fd < 0 || fd >= FS_OPEN_MAX_COUNT)? 0 : 1;
}

// returns 1 if length of 'filename' is valid, 0 otherwise
int valid_filename_len(const char *filename) {
	return (filename[0] == '\0' || 
		strlen(filename) > FS_FILENAME_LEN)? 0: 1;
}

// returns 1 if fd is currently open, 0 otherwise 
int is_open (int fd){
	if (!valid_fd(fd)) return 0;
	return fd_table[fd].fd_num != UNUSED;
}

// ------------ FAT helper functions ---------------

// return 0 if FAT is the max size
// return -1 if the total FAT size is less than max size
int read_FAT_entries(uint16_t *FAT_buf, int offset) {
	if (FAT_buf == NULL) {
		return -1;
	}

	// go through each entry in FAT
	for (int i = 0; i < BLOCK_SIZE/2; i++) {
		// dont read past the max FAT entry amt
		if ((i + offset) > MAX_FAT_READ_SIZE-1) {
			// printf("break!\n");
			return -1;
		}

		// copy each index to the array
		fat_array[i + offset] = FAT_buf[i];

		// read a non-empty entry
		if (FAT_buf[i] == FAT_EOC || FAT_buf[i] != 0) {
			// printf("%u\n", FAT_buf[i]);
			fat_frees--;
		}
	} // for

	return 0;
}

int read_blocks(uint16_t *FAT_buf) {
	// read all the blocks
	int fat_size = sblock.data_block_amt;

	// malloc array to FAT
	fat_array = (uint16_t*)malloc(fat_size*sizeof(uint16_t));

	// read each fat buffer into the array
	for (size_t j = 0; j < sblock.fat_block_amt; j++) {
		// printf("offset: %d\n", j+1);

		// read from the block
		if (block_read(j+1, FAT_buf) == -1) {
			return -1;
		}

		// read entries from the FAT buffer
		if (read_FAT_entries(FAT_buf, (BLOCK_SIZE/2)*j) == -1) {
			break;
		}
	}// for

	return 0;
}

// ---------------- API FUNCTIONS ------------------------

int fs_mount(const char *diskname) { 
	/* TODO: Phase 1 */

	if (!strlen(diskname)) {
		printf("no length\n");
		return -1;
	}

	// open virtual disk file
	if (block_disk_open(diskname) == -1) {
		disk_open = 0;
		return -1;
	}

	// read superblock
	if (block_read(0, &sblock) == -1) {
		disk_open = 0;
		return -1;
	}

	disk_open = 1;

	// FAT buffer
	uint16_t FAT_buf [BLOCK_SIZE/2];
	
	// copy the values from the superblock
	if (sblock.block_amt != block_disk_count()) {
		return -1;
	}

	if (memcmp(sblock.signature, "ECS150FS", 8) != 0) {
		return -1;
	}

	fat_frees = sblock.data_block_amt;
	MAX_FAT_READ_SIZE = sblock.data_block_amt;
	
	// read all FAT blocks
	read_blocks(FAT_buf);

	// read root directory
	if (block_read(sblock.rd_start_idx, root_dir) == -1) {
		return -1;
	}

	// look at all 128 entries in root directory
	for (size_t i = 0; i < FS_FILE_MAX_COUNT; i++) {
		// read metadata from the entry
		if (root_dir[i].name[0] != '\0') {
			root_dir_frees--;
		}
	}

	// initialize fd table
	for (size_t i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		// set to unused value
		fd_table[i].fd_num = UNUSED;
		fd_table[i].offset = UNUSED;
	}
	
	is_mounted = 1;
	return 0;
}

// check if open file descriptors
int fs_umount(void) {
	// check for open file descriptors
	for (int i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		if (is_open(i)) return -1;
	}

	if (!is_mounted || block_disk_close() == -1) {
		return -1;
	}

	// clear file descriptor table
	memset(&fd_table, -1, FS_OPEN_MAX_COUNT*sizeof(FDEntry));

	// clear root directory
	memset(&root_dir, 0, FS_FILE_MAX_COUNT*sizeof(RootDirEntry));
	root_dir_frees = FS_FILE_MAX_COUNT;

	// clear FAT
	free(fat_array);
	fat_frees = 0;

	// clear superblock
	memset(&sblock, 0, sizeof(Superblock));

	is_mounted = 0;
	return 0;
}

int fs_info(void) { 
	/* TODO: Phase 1 */

	if (!disk_open) {
		return -1;
	}

	printf("FS Info:\n");
	printf("total_blk_count=%d\n", sblock.block_amt);
	printf("fat_blk_count=%d\n", sblock.fat_block_amt);
	printf("rdir_blk=%d\n", sblock.rd_start_idx);
	printf("data_blk=%d\n", sblock.data_start_idx);
	printf("data_blk_count=%d\n", sblock.data_block_amt);
	printf("fat_free_ratio=%d/%d\n", fat_frees, sblock.data_block_amt);
	printf("rdir_free_ratio=%d/%d\n", root_dir_frees, FS_FILE_MAX_COUNT);

	return 0;
}

int fs_create(const char *filename) { 
	/* TODO: Phase 2 */

	if (!is_mounted || 
		!valid_filename_len(filename) ||
		file_exists(filename))
	{
		return -1;
	}

	int index = -1;
	// look for an empty entry in the root directory
	for (size_t i = 0; i < FS_FILE_MAX_COUNT; i++) {
		// populate the empty entry
		// NOTE: we're only make empty files for phase 2
		if (!root_dir[i].size && root_dir[i].name[0] == '\0') {
			index = i;
			break;
		}
	}

	// return if no space found
	if (index == -1) return index;
	
	strcpy(root_dir[index].name, filename);
	root_dir[index].size = 0;

	// setting to 0 means empty file
	root_dir[index].first_db_idx = FAT_EOC;
	root_dir_frees--;

	// update the root directory with the new file
	if (block_write(sblock.rd_start_idx, root_dir) == -1) return -1;

	return 0;
}

int fs_delete(const char *filename) {
	// get file descriptor of file name:
	int fd = -1;
	for (int i = 0; i < sblock.rd_start_idx; i++) {
		if (!strcmp(root_dir[i].name, filename)) {
			fd = i;
		}
	}

	/* TODO: Phase 2 */
	if (!is_mounted || 
		!valid_filename_len(filename) ||
		!file_exists(filename) ||
		is_open(fd))
	{
		return -1;
	}

	// check in the root directory
	uint16_t first_db_idx = 0;
	for (size_t i = 0; i < FS_FILE_MAX_COUNT; i++) {
		if (strcmp(filename, root_dir[i].name) == 0){
			root_dir[i].name[0] = '\0';
			root_dir[i].size = 0;
			first_db_idx = root_dir[i].first_db_idx;
			root_dir_frees++;
			break;
		}
	}
	
	//clear all FAT entries associated with the file
	int next_idx = fat_array[first_db_idx];
	while(next_idx != FAT_EOC){
		fat_array[first_db_idx] = 0;
		first_db_idx = next_idx;
		next_idx = fat_array[first_db_idx];
		fat_frees++;
	}

	fat_array[first_db_idx] = 0;
	fat_frees++;

	// write root dir back to disk
	block_write(sblock.rd_start_idx, root_dir);

	// write fat back to disk
	uint16_t temp_buf[BLOCK_SIZE/2];
	for(int i = 0; i < sblock.fat_block_amt; i++){
		block_write(i + 1, &fat_array[i*BLOCK_SIZE/2]);
		block_read(i + 1, temp_buf);
	}

	return 0;
}

int fs_ls(void) { 
	/* TODO: Phase 2 */
	if (!is_mounted) return -1;

	printf("FS Ls:\n");
	for (size_t i = 0; i < FS_FILE_MAX_COUNT; i++) {
		if (root_dir[i].name[0] != '\0') {
			printf("file: %s, size: %u, data_blk: %u\n", root_dir[i].name, root_dir[i].size, root_dir[i].first_db_idx);
		}
	}

	return 0;
}

int fs_open(const char *filename) { 
	/* TODO: Phase 3 */
	if(!is_mounted || 
		!valid_filename_len(filename) ||
		!file_exists(filename) || 
		open_files == FS_OPEN_MAX_COUNT)
	{
		return -1;
	}

	// get file index from root dir
	int root_dir_idx = 0;
	for (int i = 0; i < FS_FILE_MAX_COUNT; i++) {
		if (strcmp(root_dir[i].name, filename) == 0) {
			root_dir_idx = i;
			break;
		}
	}

	// store entry in fd_table
	int fd_idx = 0;
	for (int i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		// look for the first empty spot in the table
		if (fd_table[i].fd_num == UNUSED) 
		{
			fd_table[i].fd_num = root_dir_idx;
			fd_table[i].offset = 0;
			fd_idx = i;
			break;
		} 
	}

	open_files++;
	return fd_idx;
}

int fs_close(int fd) { 
	/* TODO: Phase 3 */ 
	if (!is_mounted ||
		!valid_fd(fd) ||
		!is_open(fd)) 
	{
		return -1;
	}
 
	// reset the fd entry to unused value
	fd_table[fd].fd_num = UNUSED;
	fd_table[fd].offset = UNUSED;

	open_files--;

	return 0;
}

int fs_stat(int fd) {
	/* TODO: Phase 3 */
	if (!is_mounted ||
		!valid_fd(fd) ||
		!is_open(fd)) 
	{
		return -1;
	}

	// read data from disk
	int ret = block_read(sblock.rd_start_idx, root_dir);
	if (ret == -1) {
		return -1;
	}

	// get the corresponding index in the root directory
	int root_dir_idx = fd_table[fd].fd_num;

	return (root_dir_idx == -1)? -1: (int)root_dir[root_dir_idx].size;
}

int fs_lseek(int fd, size_t offset) { 
	/* TODO: Phase 3 */

	if (!is_mounted || 
		!valid_fd(fd) ||
		!is_open(fd))
	{
		return -1;
	}

	// get the file size from the root directory
	block_read(sblock.rd_start_idx, root_dir);
	int root_dir_idx = fd_table[fd].fd_num;
	size_t size = root_dir[root_dir_idx].size;

	if (offset > size) return -1;

	fd_table[fd].offset = offset;

	return 0;
}

int ceiling(int a, int b) {
	return (a / b) + (a%b != 0);
}

// returns index of empty fat entry, -1 if FAT is full
int find_empty_FAT_entry(void){
	for (int i = 0; i < sblock.data_block_amt; i++){
		if (fat_array[i] == 0) return i;
	}

	return -1;
}

int fs_write(int fd, void* buf, size_t count){
	/* TODO: Phase 4 */ 
	if(!is_mounted ||
	   !valid_fd(fd) ||
	   !is_open(fd) ||
	   buf == NULL)
	{
		printf("returned -1\n");
		return -1;
	}

	if (count == 0) return 0;

	uint32_t root_dir_idx = fd_table[fd].fd_num;
	uint64_t offset = fd_table[fd].offset;
	uint16_t first_db_idx = root_dir[root_dir_idx].first_db_idx;
	uint32_t file_size = root_dir[root_dir_idx].size;
	int starting_block = offset / BLOCK_SIZE;
	int offset_in_block = !(offset % BLOCK_SIZE)? 0: 
	offset - (starting_block * BLOCK_SIZE);
	uint8_t data_written_buf [BLOCK_SIZE];

	//if first time writing to file
	int fat_empty_entry_idx;
	if (first_db_idx == FAT_EOC) {
		fat_empty_entry_idx = find_empty_FAT_entry();
		if (fat_empty_entry_idx == -1) return 0;

		first_db_idx = fat_empty_entry_idx;
		root_dir[root_dir_idx].first_db_idx = first_db_idx;
		fat_array[first_db_idx] = FAT_EOC;

		// write root directory back to disk
		if (block_write(sblock.rd_start_idx, root_dir) == -1) {
			return 0;
		}
	}

	uint16_t curr_idx = first_db_idx;
	uint16_t prev_idx = curr_idx;

	// get index of block we need to start writing to
	for(int i = 0; i < starting_block; i++){
		prev_idx = curr_idx;
		curr_idx = fat_array[(int)curr_idx];
	}

	int amt_written = 0;
	int bytes_to_write = 0;

	// while we have remaining bytes to write
	while (count > 0) {
		// if we reached end of file and we still need to write data
		if(curr_idx == FAT_EOC){
			fat_empty_entry_idx = find_empty_FAT_entry();
			if (fat_empty_entry_idx == -1) break;

			fat_array[(int) prev_idx] = fat_empty_entry_idx;
			curr_idx = fat_empty_entry_idx;
			fat_array[curr_idx] = FAT_EOC;
		}
		// read block and place in bounce buffer
		if (block_read(curr_idx + sblock.data_start_idx, &data_written_buf) == -1){
			printf("error!\n");
			break;
		}
		// calculate max amount we can write
		bytes_to_write = min(count, BLOCK_SIZE - offset_in_block);

		//write to bounce buffer and write back to disk
		memcpy(&data_written_buf[offset_in_block], buf + amt_written, bytes_to_write);
		block_write(curr_idx + sblock.data_start_idx, data_written_buf);

		// update variables
		offset_in_block = 0;
		amt_written += bytes_to_write;
		count -= bytes_to_write;
		offset += bytes_to_write;
		prev_idx = curr_idx;
		curr_idx = fat_array[(int)prev_idx];
	}
	
	// update size of root directory then write back to disk
	root_dir[root_dir_idx].size = (offset > file_size)? offset: file_size;
	fd_table[fd].offset = offset;

	// write root dir back to disk
	block_write(sblock.rd_start_idx, root_dir);

	// write fat back to disk
	uint16_t temp_buf[BLOCK_SIZE/2];
	for(int i = 0; i < sblock.fat_block_amt; i++){
		block_write(i + 1, &fat_array[i*BLOCK_SIZE/2]);
		block_read(i + 1, temp_buf);
	}

	return amt_written;
}

int fs_read(int fd, void *buf, size_t count) { 
	/* TODO: Phase 4 */ 
	if(!is_mounted ||
	   !valid_fd(fd) ||
	   !is_open(fd) ||
	   buf == NULL)
	{
		return -1;
	}

	uint32_t root_dir_idx = fd_table[fd].fd_num;
	uint64_t offset = fd_table[fd].offset;
	uint16_t first_db_idx = root_dir[root_dir_idx].first_db_idx;
	uint32_t file_size = root_dir[root_dir_idx].size;
	int db_amt = ceiling(count, BLOCK_SIZE);
	uint8_t data_read_buf [BLOCK_SIZE];
	int starting_block = offset / BLOCK_SIZE;
	int offset_in_block = !(offset % BLOCK_SIZE)? 0: offset - (starting_block * BLOCK_SIZE);
	uint16_t curr_idx = first_db_idx;

	// get index of block we need to start reading from
	for (int i = 0; i < starting_block; i++){
		curr_idx = fat_array[(int)curr_idx];
	}

	int amt_read = 0;
	int dbs_read = 0;
	uint64_t bytes_to_read = 0;

	while (dbs_read < db_amt) {
		if(curr_idx == FAT_EOC)
			break;

		// read from current block and place in bounce buffer
		if (block_read(curr_idx + sblock.data_start_idx, data_read_buf) == -1){
			printf("error!\n");
			break;
		}

		// calculate max amount we can read
		bytes_to_read = min(count, BLOCK_SIZE - offset_in_block);
		bytes_to_read = (bytes_to_read + offset > file_size)? file_size - offset: bytes_to_read;

		// read from bounce buffer
		memcpy(buf + amt_read, &data_read_buf[offset_in_block], bytes_to_read);

		// update variables
		amt_read += bytes_to_read;
		count -= bytes_to_read;
		offset += bytes_to_read;
		offset_in_block = 0;
		dbs_read ++;
		curr_idx = fat_array[(int)curr_idx];
	}

	// update offset
	fd_table[fd].offset += offset;

	return amt_read;
}