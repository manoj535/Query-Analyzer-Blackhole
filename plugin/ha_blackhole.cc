/* Copyright (c) 2005, 2011, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation				// gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "sql_priv.h"
#include "unireg.h"
#include "probes_mysql.h"
#include "ha_blackhole.h"
#include "sql_class.h"                          // THD, SYSTEM_THREAD_SLAVE_SQL

/*#if defined(SAFE_MUTEX)
#undef SAFE_MUTEX
#endif*/  

/* Static declarations for handlerton */

static bool is_slave_applier(THD *thd)
{
  return thd->system_thread == SYSTEM_THREAD_SLAVE_SQL ||
    thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER;
}


static handler *blackhole_create_handler(handlerton *hton,
                                         TABLE_SHARE *table,
                                         MEM_ROOT *mem_root)
{
  return new (mem_root) qa_blackhole(hton, table);
}


/* Static declarations for shared structures */

static mysql_mutex_t blackhole_mutex;
static HASH blackhole_open_tables;

static st_blackhole_share *get_share(const char *table_name);
static void free_share(st_blackhole_share *share);

/*****************************************************************************
** BLACKHOLE tables
*****************************************************************************/

qa_blackhole::qa_blackhole(handlerton *hton,
                           TABLE_SHARE *table_arg)
  :handler(hton, table_arg),kRowCountFilePath("/tmp/rowcount.txt")
{
	mDefaultRowCount=10000;
	
}


static const char *qa_blackhole_exts[] = {
  NullS
};

const char **qa_blackhole::bas_ext() const
{
  return qa_blackhole_exts;
}

int qa_blackhole::open(const char *name, int mode, uint test_if_locked)
{
	unsigned lFound;
	DBUG_ENTER("qa_blackhole::open");
	
	mTableName = name;
	lFound = mTableName.find_last_of("/\\");
	if(lFound != std::string::npos)
	{
		mTableName = mTableName.substr(lFound+1);
	} // if
	printf("qa_blackhole::open:%s\n",mTableName.c_str());
	if (!(share= get_share(name)))
		DBUG_RETURN(HA_ERR_OUT_OF_MEM);

	thr_lock_data_init(&share->lock, &lock, NULL);
	DBUG_RETURN(0);
}

int qa_blackhole::close(void)
{
  DBUG_ENTER("qa_blackhole::close");
  printf("qa_blackhole::close\n");
  free_share(share);
  DBUG_RETURN(0);
}

int qa_blackhole::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("qa_blackhole::create");
  printf("qa_blackhole::create\n");
  DBUG_RETURN(0);
}

/*
  Intended to support partitioning.
  Allows a particular partition to be truncated.
*/
int qa_blackhole::truncate()
{
  DBUG_ENTER("qa_blackhole::truncate");
  printf("qa_blackhole::truncate\n");
  DBUG_RETURN(0);
}

const char *qa_blackhole::index_type(uint key_number)
{
  DBUG_ENTER("qa_blackhole::index_type");
  printf("qa_blackhole::index_type\n");
  DBUG_RETURN((table_share->key_info[key_number].flags & HA_FULLTEXT) ? 
              "FULLTEXT" :
              (table_share->key_info[key_number].flags & HA_SPATIAL) ?
              "SPATIAL" :
              (table_share->key_info[key_number].algorithm ==
               HA_KEY_ALG_RTREE) ? "RTREE" : "BTREE");
}

int qa_blackhole::write_row(uchar * buf)
{
  DBUG_ENTER("qa_blackhole::write_row");
  printf("qa_blackhole::write_row\n");
  ha_statistic_increment(&SSV::ha_write_count);
  DBUG_RETURN(table->next_number_field ? update_auto_increment() : 0);
}

int qa_blackhole::update_row(const uchar *old_data, uchar *new_data)
{
  DBUG_ENTER("qa_blackhole::update_row");
  printf("qa_blackhole::update_row\n");
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
 if (is_slave_applier(thd) && thd->query() == NULL)
    DBUG_RETURN(0);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int qa_blackhole::delete_row(const uchar *buf)
{
  DBUG_ENTER("qa_blackhole::delete_row");
  printf("qa_blackhole::delete_row\n");
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  ha_statistic_increment(&SSV::ha_delete_count);
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
 if (is_slave_applier(thd) && thd->query() == NULL)
    DBUG_RETURN(0);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int qa_blackhole::rnd_init(bool scan)
{
  DBUG_ENTER("qa_blackhole::rnd_init");
  printf("qa_blackhole::rnd_init\n");
  DBUG_RETURN(0);
}


int qa_blackhole::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("qa_blackhole::rnd_next");
  printf("qa_blackhole::rnd_next\n");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
 if (is_slave_applier(thd) && thd->query() == NULL)
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
    //rc=0;
  MYSQL_READ_ROW_DONE(rc);
  table->status= rc ? STATUS_NOT_FOUND : 0;
  DBUG_RETURN(rc);
}


int qa_blackhole::rnd_pos(uchar * buf, uchar *pos)
{
  DBUG_ENTER("qa_blackhole::rnd_pos");
  printf("qa_blackhole::rnd_pos\n");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       FALSE);
  DBUG_ASSERT(0);
  MYSQL_READ_ROW_DONE(0);
  DBUG_RETURN(0);
}


void qa_blackhole::position(const uchar *record)
{
  DBUG_ENTER("qa_blackhole::position");
  printf("qa_blackhole::position\n");
  DBUG_ASSERT(0);
  DBUG_VOID_RETURN;
}


int qa_blackhole::info(uint flag)
{
  DBUG_ENTER("qa_blackhole::info");
  printf("qa_blackhole::info:%s\n",mTableName.c_str());
  memset(&stats, 0, sizeof(stats));
  
  if (flag & HA_STATUS_VARIABLE)
  {
	//mysql_mutex_lock(&blackhole_mutex);  
	ifstream lRowCountFile(kRowCountFilePath.c_str()); 
	string lLine;
	size_t lFound;
	int lRowCount = 0;
	string lTemp;
	if(lRowCountFile)
	{
		while(std::getline(lRowCountFile, lLine))
		{
			lFound = lLine.find(mTableName);
			if(lFound != string::npos)
			{
				lTemp=lLine.substr(mTableName.length()+1);
				lRowCount = atoi(lTemp.c_str());
				break;
			} // if
			
		} // while
		if(lRowCount)
		{
			stats.records=lRowCount;
			cout<<"rowcount for table:"<<mTableName<<" is :"<<lRowCount<<endl;
		}
		else
		{
			stats.records=mDefaultRowCount;
			cout<<"setting default row count:"<<mDefaultRowCount<<" for table:"<<mTableName<<endl;
		}
		
		lRowCountFile.close();
	}
	else
	{
		cout<<"row count file not found. setting global rowcount:"<<mDefaultRowCount<<endl;
		stats.records=mDefaultRowCount;
	}
    //stats.deleted=9999;
    //mysql_mutex_unlock(&blackhole_mutex);
  }
  
  if (flag & HA_STATUS_AUTO)
    stats.auto_increment_value= 1;
  DBUG_RETURN(0);
}

ha_rows qa_blackhole::records_in_range(uint inx,key_range *min_key,key_range *max_key)
{
	printf("qa_blackhole::records_in_range:%d\n",inx);
	return BLACKHOLE_REC_RANGE;
}

/*Item * qa_blackhole::idx_cond_push(uint keyno, Item* idx_cond) 
{ 
	printf("qa_blackhole::idx_cond_push\n");
	return idx_cond; 
}*/

/*int qa_blackhole::rnd_same(uchar *buf, uint inx)
{ 
	printf("qa_blackhole::rnd_same\n");
	return HA_ERR_WRONG_COMMAND; 
}*/

int qa_blackhole::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("qa_blackhole::external_lock");
  printf("qa_blackhole::external_lock\n");
  DBUG_RETURN(0);
}


THR_LOCK_DATA **qa_blackhole::store_lock(THD *thd,
                                         THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type)
{
  DBUG_ENTER("qa_blackhole::store_lock");
  printf("qa_blackhole::store_lock\n");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    /*
      Here is where we get into the guts of a row level lock.
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT &&
         lock_type <= TL_WRITE) && !thd_in_lock_tables(thd)
        && !thd_tablespace_op(thd))
      lock_type = TL_WRITE_ALLOW_WRITE;

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
      lock_type = TL_READ;

    lock.type= lock_type;
  }
  *to++= &lock;
  DBUG_RETURN(to);
}


int qa_blackhole::index_read_map(uchar * buf, const uchar * key,
                                 key_part_map keypart_map,
                             enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_read_map");
  printf("qa_blackhole::index_read_map\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  ha_statistic_increment(&SSV::ha_read_key_count);
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
  if (is_slave_applier(thd) && thd->query() == NULL)
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= rc ? STATUS_NOT_FOUND : 0;
  DBUG_RETURN(rc);
  
}

/*int qa_blackhole::index_read_idx_map(uchar * buf, uint idx, const uchar * key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_read_idx");
  printf("qa_blackhole::index_read_idx_map\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
  if (is_slave_applier(thd) && thd->query() == NULL)
    rc= 0;
  else
    //rc = 0;
    rc= HA_ERR_END_OF_FILE;
    //rc=0;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  printf("rc:%d:table_status:%d\n",rc,table->status);
  table->status= rc ? STATUS_NOT_FOUND : 0;
  DBUG_RETURN(rc);
  //printf("qa_blackhole::index_read_idx\n");
  //return 0;
}*/


int qa_blackhole::index_read_last_map(uchar * buf, const uchar * key,
                                      key_part_map keypart_map)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_read_last");
  printf("qa_blackhole::index_read_last\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
  if (is_slave_applier(thd) && thd->query() == NULL)
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= rc ? STATUS_NOT_FOUND : 0;
  DBUG_RETURN(rc);
}


int qa_blackhole::index_next(uchar * buf)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_next");
  printf("qa_blackhole::index_next\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}


int qa_blackhole::index_prev(uchar * buf)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_prev");
  printf("qa_blackhole::index_prev\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}


int qa_blackhole::index_first(uchar * buf)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_first");
  printf("qa_blackhole::index_first\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}


int qa_blackhole::index_last(uchar * buf)
{
  int rc;
  DBUG_ENTER("qa_blackhole::index_last");
  printf("qa_blackhole::index_last\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}

/*int qa_blackhole::analyze
		(THD* thd, HA_CHECK_OPT* check_opt)
{
	printf("qa_blackhole::analyze\n");
	return 0;
}
int qa_blackhole::optimize(THD* thd, HA_CHECK_OPT* check_opt)
{
	printf("qa_blackhole::optimize\n");
	return 0;
}
*/

static st_blackhole_share *get_share(const char *table_name)
{
  st_blackhole_share *share;
  uint length;

  length= (uint) strlen(table_name);
  mysql_mutex_lock(&blackhole_mutex);
    
  if (!(share= (st_blackhole_share*)
        my_hash_search(&blackhole_open_tables,
                       (uchar*) table_name, length)))
  {
    if (!(share= (st_blackhole_share*) my_malloc(sizeof(st_blackhole_share) +
                                                 length,
                                                 MYF(MY_WME | MY_ZEROFILL))))
      goto error;

    share->table_name_length= length;
    strmov(share->table_name, table_name);
    
    if (my_hash_insert(&blackhole_open_tables, (uchar*) share))
    {
      my_free(share);
      share= NULL;
      goto error;
    }
    
    thr_lock_init(&share->lock);
  }
  share->use_count++;
  
error:
  mysql_mutex_unlock(&blackhole_mutex);
  return share;
}

static void free_share(st_blackhole_share *share)
{
  mysql_mutex_lock(&blackhole_mutex);
  if (!--share->use_count)
    my_hash_delete(&blackhole_open_tables, (uchar*) share);
  mysql_mutex_unlock(&blackhole_mutex);
}

static void blackhole_free_key(st_blackhole_share *share)
{
  thr_lock_delete(&share->lock);
  my_free(share);
}

static uchar* blackhole_get_key(st_blackhole_share *share, size_t *length,
                                my_bool not_used __attribute__((unused)))
{
  *length= share->table_name_length;
  return (uchar*) share->table_name;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key bh_key_mutex_blackhole;

static PSI_mutex_info all_blackhole_mutexes[]=
{
  { &bh_key_mutex_blackhole, "blackhole", PSI_FLAG_GLOBAL}
};

void init_qa_blackhole_psi_keys()
{
  const char* category= "blackhole";
  int count;

  /*if (PSI_server == NULL)
    return;
*/
  count= array_elements(all_blackhole_mutexes);
  //PSI_server->register_mutex(category, all_blackhole_mutexes, count);
  mysql_mutex_register(category, all_blackhole_mutexes, count);

}
#endif

static int qa_blackhole_init(void *p)
{
  printf("qa_blackhole_init\n");	
  handlerton *blackhole_hton;

#ifdef HAVE_PSI_INTERFACE
  init_qa_blackhole_psi_keys();
#endif

  blackhole_hton= (handlerton *)p;
  blackhole_hton->state= SHOW_OPTION_YES;
  blackhole_hton->db_type= DB_TYPE_BLACKHOLE_DB;
  blackhole_hton->create= blackhole_create_handler;
  blackhole_hton->flags= HTON_CAN_RECREATE;

  //printf("initializes the blackhole\n");
  //CHARSET_INFO *my_system_charset_info = &my_charset_utf8_bin;
  CHARSET_INFO *my_system_charset_info = &my_charset_utf8_general_ci;
  
  mysql_mutex_init(bh_key_mutex_blackhole,
                   &blackhole_mutex, MY_MUTEX_INIT_FAST);
  my_hash_init(&blackhole_open_tables,my_system_charset_info,32,0,0,
                      (my_hash_get_key) blackhole_get_key,
                      (my_hash_free_key) blackhole_free_key, 0);

  return 0;
}

static int qa_blackhole_fini(void *p)
{
  my_hash_free(&blackhole_open_tables);
  mysql_mutex_destroy(&blackhole_mutex);

  return 0;
}

struct st_mysql_storage_engine qa_blackhole_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(qa_blackhole)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &qa_blackhole_storage_engine,
  "QA_BLACKHOLE",
  "MySQL AB",
  "/dev/null storage engine (anything you write to it disappears)",
  PLUGIN_LICENSE_GPL,
  qa_blackhole_init, /* Plugin Init */
  qa_blackhole_fini, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  NULL,                       /* status variables                */
  NULL,        				  /* system variables                */
  NULL,                       /* config options                  */
  0,                          /* flags                           */
}
mysql_declare_plugin_end;
