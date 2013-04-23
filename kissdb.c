/* (Keep It) Simple Stupid Database
 *
 * Written by Adam Ierymenko <adam.ierymenko@zerotier.com>
 * KISSDB is in the public domain and is distributed with NO WARRANTY. */

/* Compile with KISSDB_TEST to build as a test program. */

/* Note: big-endian systems will need changes to implement byte swapping
 * on hash table file I/O. */

#define _FILE_OFFSET_BITS 64

#include "kissdb.h"

#include <string.h>
#include <stdlib.h>

/* djb2 hash function */
static uint64_t KISSDB_hash(const void *b,unsigned long len)
{
	unsigned long i;
	uint64_t hash = 5381;
	for(i=0;i<len;++i)
		hash = ((hash << 5) + hash) + (uint64_t)(((const uint8_t *)b)[i]);
	return hash;
}

int KISSDB_open(
	KISSDB *db,
	const char *path,
	int mode,
	unsigned long hash_table_size,
	unsigned long key_size,
	unsigned long value_size)
{
	uint64_t *httmp;

	db->hash_table_size = hash_table_size;
	db->key_size = key_size;
	db->value_size = value_size;
	db->hash_table_size_bytes = sizeof(uint64_t) * (hash_table_size + 1); /* [hash_table_size] == next table */

	db->f = fopen(path,((mode == KISSDB_OPEN_MODE_RWREPLACE) ? "w+b" : ((mode == KISSDB_OPEN_MODE_RDWR) ? "r+b" : "rb")));
	if (!db->f) {
		if (mode == KISSDB_OPEN_MODE_RWCREAT)
			db->f = fopen(path,"w+b");
		if (!db->f)
			return -1;
	}

	httmp = malloc(db->hash_table_size_bytes);
	db->num_hash_tables = 0;
	db->hash_tables = (uint64_t *)0;
	while (fread(httmp,db->hash_table_size_bytes,1,db->f) == 1) {
		db->hash_tables = realloc(db->hash_tables,db->hash_table_size_bytes * (db->num_hash_tables + 1));
		memcpy(((uint8_t *)db->hash_tables) + (db->hash_table_size_bytes * db->num_hash_tables),httmp,db->hash_table_size_bytes);
		++db->num_hash_tables;
		if (httmp[db->hash_table_size]) {
			if (fseeko(db->f,httmp[db->hash_table_size],SEEK_SET)) {
				KISSDB_close(db);
				return -1;
			}
		} else break;
	}
	free(httmp);

	return 0;
}

void KISSDB_close(KISSDB *db)
{
	if (db->hash_tables)
		free(db->hash_tables);
	if (db->f)
		fclose(db->f);
	memset(db,0,sizeof(KISSDB));
}

int KISSDB_get(KISSDB *db,const void *key,void *vbuf)
{
	uint8_t tmp[1024];
	const uint8_t *kptr;
	unsigned long klen,i;
	uint64_t hash = KISSDB_hash(key,db->key_size) % (uint64_t)db->hash_table_size;
	uint64_t offset;
	uint64_t *cur_hash_table;
	long n;

	cur_hash_table = db->hash_tables;
	for(i=0;i<db->num_hash_tables;++i) {
		offset = cur_hash_table[hash];
		if (offset) {
			if (fseeko(db->f,offset,SEEK_SET))
				return -1; /* I/O error */

			kptr = (const uint8_t *)key;
			klen = db->key_size;
			while (klen) {
				n = fread(tmp,1,(klen > sizeof(tmp)) ? sizeof(tmp) : klen,db->f);
				if (n > 0) {
					if (memcmp(kptr,tmp,n))
						goto get_no_match_next_hash_table;
					kptr += n;
					klen -= (unsigned long)n;
				} else return 1; /* not found */
			}

			if (fread(vbuf,db->value_size,1,db->f) == 1)
				return 0; /* success */
			else return -1; /* I/O error */
		} else return 1; /* not found */
get_no_match_next_hash_table:
		cur_hash_table += db->hash_table_size + 1;
	}

	return 1; /* not found */
}

int KISSDB_put(KISSDB *db,const void *key,const void *value)
{
	uint8_t tmp[1024];
	const uint8_t *kptr;
	unsigned long klen,i;
	uint64_t hash = KISSDB_hash(key,db->key_size) % (uint64_t)db->hash_table_size;
	uint64_t offset;
	uint64_t htoffset,lasthtoffset;
	uint64_t endoffset;
	uint64_t *cur_hash_table;
	long n;

	lasthtoffset = 0;
	htoffset = 0;
	cur_hash_table = db->hash_tables;
	for(i=0;i<db->num_hash_tables;++i) {
		offset = cur_hash_table[hash];
		if (offset) {
			/* rewrite if already exists */
			if (fseeko(db->f,offset,SEEK_SET))
				return -1; /* I/O error */

			kptr = (const uint8_t *)key;
			klen = db->key_size;
			while (klen) {
				n = fread(tmp,1,(klen > sizeof(tmp)) ? sizeof(tmp) : klen,db->f);
				if (n > 0) {
					if (memcmp(kptr,tmp,n))
						goto put_no_match_next_hash_table;
					kptr += n;
					klen -= (unsigned long)n;
				}
			}

			if (fwrite(value,db->value_size,1,db->f) == 1)
				return 0; /* success */
			else return -1; /* I/O error */
		} else {
			/* add if an empty hash table slot is discovered */
			if (fseeko(db->f,0,SEEK_END))
				return -1; /* I/O error */
			endoffset = ftello(db->f);

			if (fwrite(key,db->key_size,1,db->f) != 1)
				return -1; /* I/O error */
			if (fwrite(value,db->value_size,1,db->f) != 1)
				return -1; /* I/O error */

			if (fseeko(db->f,htoffset + (sizeof(uint64_t) * hash),SEEK_SET))
				return -1; /* I/O error */
			if (fwrite(&endoffset,sizeof(uint64_t),1,db->f) != 1)
				return -1; /* I/O error */
			cur_hash_table[hash] = endoffset;

			fflush(db->f);

			return 0; /* success */
		}
put_no_match_next_hash_table:
		lasthtoffset = htoffset;
		htoffset = cur_hash_table[db->hash_table_size];
		cur_hash_table += (db->hash_table_size + 1);
	}

	/* if no existing slots, add a new page of hash table entries */
	if (fseeko(db->f,0,SEEK_END))
		return -1; /* I/O error */
	endoffset = ftello(db->f);

	db->hash_tables = realloc(db->hash_tables,db->hash_table_size_bytes * (db->num_hash_tables + 1));
	cur_hash_table = &(db->hash_tables[(db->hash_table_size + 1) * db->num_hash_tables]);
	memset(cur_hash_table,0,db->hash_table_size_bytes);

	cur_hash_table[hash] = endoffset + db->hash_table_size_bytes; /* where new entry will go */

	if (fwrite(cur_hash_table,db->hash_table_size_bytes,1,db->f) != 1) {
		db->hash_tables = realloc(db->hash_tables,db->hash_table_size_bytes * db->num_hash_tables);
		return -1; /* I/O error */
	}

	if (fwrite(key,db->key_size,1,db->f) != 1)
		return -1; /* I/O error */
	if (fwrite(value,db->value_size,1,db->f) != 1)
		return -1; /* I/O error */

	if (db->num_hash_tables) {
		if (fseeko(db->f,lasthtoffset + (sizeof(uint64_t) * db->hash_table_size),SEEK_SET))
			return -1; /* I/O error */
		if (fwrite(&endoffset,sizeof(uint64_t),1,db->f) != 1)
			return -1; /* I/O error */
		db->hash_tables[((db->hash_table_size + 1) * (db->num_hash_tables - 1)) + db->hash_table_size] = endoffset;
	}

	++db->num_hash_tables;

	return 0; /* success */
}

void KISSDB_Iterator_init(KISSDB *db,KISSDB_Iterator *dbi)
{
	dbi->db = db;
	dbi->h_no = 0;
	dbi->h_idx = 0;
}

int KISSDB_Iterator_next(KISSDB_Iterator *dbi,void *kbuf,void *vbuf)
{
	uint64_t offset;

	if ((dbi->h_no < dbi->db->num_hash_tables)&&(dbi->h_idx < dbi->db->hash_table_size)) {
		while (!(offset = dbi->db->hash_tables[((dbi->db->hash_table_size + 1) * dbi->h_no) + dbi->h_idx])) {
			if (++dbi->h_idx >= dbi->db->hash_table_size) {
				dbi->h_idx = 0;
				if (++dbi->h_no >= dbi->db->num_hash_tables)
					return 0;
			}
		}
		if (fseeko(dbi->db->f,offset,SEEK_SET))
			return -1;
		if (fread(kbuf,dbi->db->key_size,1,dbi->db->f) != 1)
			return -1;
		if (fread(vbuf,dbi->db->value_size,1,dbi->db->f) != 1)
			return -1;
		if (++dbi->h_idx >= dbi->db->hash_table_size) {
			dbi->h_idx = 0;
			++dbi->h_no;
		}
		return 1;
	}

	return 0;
}

#ifdef KISSDB_TEST

int main(int argc,char **argv)
{
	uint64_t i,j;
	uint64_t v[8];
	KISSDB db;
	KISSDB_Iterator dbi;
	char got_all_values[10000];
	int q;

	printf("Opening new empty database test.db...\n");

	if (KISSDB_open(&db,"test.db",KISSDB_OPEN_MODE_RWREPLACE,1024,8,sizeof(v))) {
		printf("KISSDB_open failed\n");
		return -1;
	}

	printf("Adding and then re-getting 10000 64-byte values...\n");

	for(i=0;i<10000;++i) {
		for(j=0;j<8;++j)
			v[j] = i;
		if (KISSDB_put(&db,&i,v)) {
			printf("KISSDB_put failed (%llu)\n",i);
			return -1;
		}
		memset(v,0,sizeof(v));
		if ((q = KISSDB_get(&db,&i,v))) {
			printf("KISSDB_get (1) failed (%llu) (%d)\n",i,q);
			return -1;
		}
		for(j=0;j<8;++j) {
			if (v[j] != i) {
				printf("KISSDB_get (1) failed, bad data (%llu)\n",i);
				return -1;
			}
		}
	}

	printf("Getting 10000 64-byte values...\n");

	for(i=0;i<10000;++i) {
		if ((q = KISSDB_get(&db,&i,v))) {
			printf("KISSDB_get (2) failed (%llu) (%d)\n",i,q);
			return -1;
		}
		for(j=0;j<8;++j) {
			if (v[j] != i) {
				printf("KISSDB_get (2) failed, bad data (%llu)\n",i);
				return -1;
			}
		}
	}

	printf("Closing and re-opening database in read-only mode...\n");

	KISSDB_close(&db);

	if (KISSDB_open(&db,"test.db",KISSDB_OPEN_MODE_RDONLY,1024,8,sizeof(v))) {
		printf("KISSDB_open failed\n");
		return -1;
	}

	printf("Getting 10000 64-byte values...\n");

	for(i=0;i<10000;++i) {
		if ((q = KISSDB_get(&db,&i,v))) {
			printf("KISSDB_get (3) failed (%llu) (%d)\n",i,q);
			return -1;
		}
		for(j=0;j<8;++j) {
			if (v[j] != i) {
				printf("KISSDB_get (3) failed, bad data (%llu)\n",i);
				return -1;
			}
		}
	}

	printf("Iterator test...\n");

	KISSDB_Iterator_init(&db,&dbi);
	i = 0xdeadbeef;
	memset(got_all_values,0,sizeof(got_all_values));
	while (KISSDB_Iterator_next(&dbi,&i,&v) > 0) {
		if (i < 10000)
			got_all_values[i] = 1;
		else {
			printf("KISSDB_Iterator_next failed, bad data (%llu)\n",i);
			return -1;
		}
	}
	for(i=0;i<10000;++i) {
		if (!got_all_values[i]) {
			printf("KISSDB_Iterator failed, missing value index %llu\n",i);
			return -1;
		}
	}

	KISSDB_close(&db);

	printf("All tests OK!\n");

	return 0;
}

#endif
