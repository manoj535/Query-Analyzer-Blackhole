
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation				// gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "sql_priv.h"
#include "unireg.h"
#include "probes_mysql.h"
#include "ha_fakeengine.h"
#include "sql_class.h"                          // THD, SYSTEM_THREAD_SLAVE_SQL

/* Static declarations for handlerton */

static bool is_slave_applier(THD *thd)
{
  return thd->system_thread == SYSTEM_THREAD_SLAVE_SQL ||
    thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER;
}


static handler *fakeengine_create_handler(handlerton *hton,
                                         TABLE_SHARE *table,
                                         MEM_ROOT *mem_root)
{
  return new (mem_root) ha_fakeengine(hton, table);
}


/* Static declarations for shared structures */

static mysql_mutex_t fakeengine_mutex;
static HASH fakeengine_open_tables;

static st_fakeengine_share *get_share(const char *table_name);
static void free_share(st_fakeengine_share *share);

/*****************************************************************************
** BLACKHOLE tables
*****************************************************************************/

ha_fakeengine::ha_fakeengine(handlerton *hton,
                           TABLE_SHARE *table_arg)
  :handler(hton, table_arg),kRowCountFilePath("/tmp/rowcount.txt"),
  kIndexCardinalityFilePath("/tmp/indexcount.txt"), kIndexRangeFilePath("/tmp/indexrangecount.txt")
{
	mDefaultRowCount=10000;
	
}


static const char *ha_fakeengine_exts[] = {
  NullS
};

const char **ha_fakeengine::bas_ext() const
{
  return ha_fakeengine_exts;
}

int ha_fakeengine::open(const char *name, int mode, uint test_if_locked)
{
	unsigned lFound;
	DBUG_ENTER("ha_fakeengine::open");
	
	mTableName = name;
	lFound = mTableName.find_last_of("/\\");
	if(lFound != std::string::npos)
	{
		mTableName = mTableName.substr(lFound+1);
	} // if
	
	if (!(share= get_share(name)))
		DBUG_RETURN(HA_ERR_OUT_OF_MEM);

	thr_lock_data_init(&share->lock, &lock, NULL);
	DBUG_RETURN(0);
}

int ha_fakeengine::close(void)
{
  DBUG_ENTER("ha_fakeengine::close");
  //printf("ha_fakeengine::close\n");
  free_share(share);
  DBUG_RETURN(0);
}

int ha_fakeengine::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_fakeengine::create");
  //printf("ha_fakeengine::create\n");
  DBUG_RETURN(0);
}

/*
  Intended to support partitioning.
  Allows a particular partition to be truncated.
*/
int ha_fakeengine::truncate()
{
  DBUG_ENTER("ha_fakeengine::truncate");
  //printf("ha_fakeengine::truncate\n");
  DBUG_RETURN(0);
}

const char *ha_fakeengine::index_type(uint key_number)
{
  DBUG_ENTER("ha_fakeengine::index_type");
  //printf("ha_fakeengine::index_type\n");
  DBUG_RETURN((table_share->key_info[key_number].flags & HA_FULLTEXT) ? 
              "FULLTEXT" :
              (table_share->key_info[key_number].flags & HA_SPATIAL) ?
              "SPATIAL" :
              (table_share->key_info[key_number].algorithm ==
               HA_KEY_ALG_RTREE) ? "RTREE" : "BTREE");
}

int ha_fakeengine::write_row(uchar * buf)
{
  DBUG_ENTER("ha_fakeengine::write_row");
  //printf("ha_fakeengine::write_row\n");
  ha_statistic_increment(&SSV::ha_write_count);
  DBUG_RETURN(table->next_number_field ? update_auto_increment() : 0);
}

int ha_fakeengine::update_row(const uchar *old_data, uchar *new_data)
{
  DBUG_ENTER("ha_fakeengine::update_row");
  //printf("ha_fakeengine::update_row\n");
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
 if (is_slave_applier(thd) && thd->query() == NULL)
    DBUG_RETURN(0);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_fakeengine::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_fakeengine::delete_row");
  //printf("ha_fakeengine::delete_row\n");
  //THD *thd= ha_thd();
  THD *thd=table->in_use;
  ha_statistic_increment(&SSV::ha_delete_count);
  //if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
 if (is_slave_applier(thd) && thd->query() == NULL)
    DBUG_RETURN(0);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_fakeengine::rnd_init(bool scan)
{
  DBUG_ENTER("ha_fakeengine::rnd_init");
  //printf("ha_fakeengine::rnd_init\n");
  DBUG_RETURN(0);
}


int ha_fakeengine::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::rnd_next");
  //printf("ha_fakeengine::rnd_next\n");
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


int ha_fakeengine::rnd_pos(uchar * buf, uchar *pos)
{
  DBUG_ENTER("ha_fakeengine::rnd_pos");
  //printf("ha_fakeengine::rnd_pos\n");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       FALSE);
  DBUG_ASSERT(0);
  MYSQL_READ_ROW_DONE(0);
  DBUG_RETURN(0);
}


void ha_fakeengine::position(const uchar *record)
{
  DBUG_ENTER("ha_fakeengine::position");
  //printf("ha_fakeengine::position\n");
  DBUG_ASSERT(0);
  DBUG_VOID_RETURN;
}


int ha_fakeengine::info(uint flag)
{
	DBUG_ENTER("ha_fakeengine::info");
	//printf("ha_fakeengine::info:%s\n",mTableName.c_str());
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
				//cout<<"rowcount for table:"<<mTableName<<" is :"<<lRowCount<<endl;
			}
			else
			{
				stats.records=mDefaultRowCount;
				//cout<<"setting default row count:"<<mDefaultRowCount<<" for table:"<<mTableName<<endl;
			}
		
			lRowCountFile.close();
		} // if
		else
		{
			//cout<<"row count file not found. setting global rowcount:"<<mDefaultRowCount<<endl;
			stats.records=mDefaultRowCount;
		}
		//stats.deleted=9999;
		//mysql_mutex_unlock(&blackhole_mutex);
	} // if
	
	// set index cardinality
	if (flag & HA_STATUS_VARIABLE)
	{
		string lIndexName;
		int lIndexCardinality = 0;
		for (int i = 0; i < table->s->keys; i++) 
		{
			lIndexName = table->key_info[i].name;
			lIndexCardinality = getIndexCount(kIndexCardinalityFilePath, lIndexName);
			// if cardinality is not mentioned in file, by default make index as unique index
			if(!lIndexCardinality)
			lIndexCardinality = stats.records;
			for (int j = 0; j < table->key_info[i].actual_key_parts; j++) 
			{
				if(lIndexName=="PRIMARY")
				{
					table->key_info[i].rec_per_key[j] = 1;
				}
				else
				{
					table->key_info[i].rec_per_key[j] = stats.records/lIndexCardinality;
					//lIndexList.push_back(lIndexName);
				}
			} // for
		} // for
	}
  
	if (flag & HA_STATUS_AUTO)
		stats.auto_increment_value= 1;
	DBUG_RETURN(0);
}

/*double ha_fakeengine::scan_time()
{
	cout<<"scan_time"<<endl;
	return 100000000;
}*/

/*double ha_fakeengine::read_time(uint index, uint ranges, ha_rows rows)
{
	cout<<"ha_fakeengine::read_time"<<endl;
	return 1000;
}*/

/*ha_rows 
ha_fakeengine::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                     void *seq_init_param, uint n_ranges_arg,
                                     uint *bufsz, uint *flags, 
                                     Cost_estimate *cost)
{
	cout<<"multi_range_read_info_const"<<endl;
    return records_in_range(keyno, NULL, NULL);
}*/

/*double ha_fakeengine::index_only_read_time(uint keynr, double records)
{
	cout<<"index_only_read_time"<<endl;
	return 1000;
}*/

ha_rows ha_fakeengine::records_in_range(uint inx,key_range *min_key,key_range *max_key)
{
	string lIndexName = table->key_info[inx].name;
	//cout<<"records_in_range:table:"<<mTableName<<":"<<lIndexName<<endl;
	long lResult = getIndexCount(kIndexRangeFilePath,lIndexName);
	if(!lResult)
	return FAKEENGINE_REC_RANGE;
	else
	return lResult;
}

long ha_fakeengine::getIndexCount(string iFilePath, string iIndexName)
{
	size_t lFound,lFoundComma;
	long lResult = 0;
	fstream  lCountFile;
	string lTableInFile;
	string lIndexInFile;
	long lCountInFile = 0;
	string lLine;
	lCountFile.open(iFilePath.c_str(), ios::in);
	if(lCountFile.good())
	{
		while(getline(lCountFile, lLine))
		{
			lFound = lLine.find(",");
			lTableInFile = lLine.substr(0,lFound);
			lFoundComma = lLine.find("=");
			lIndexInFile = lLine.substr(lFound+1,lFoundComma-lFound-1);
			lCountInFile = atoi((lLine.substr(lFoundComma+1).c_str()));
			
			//if(lTableInFile == mTableName && lIndexInFile == lIndexName)
			if(!strncasecmp(lTableInFile.c_str(), mTableName.c_str(),mTableName.length()) &&
					!strncasecmp(lIndexInFile.c_str(), iIndexName.c_str(),iIndexName.length()) )
			{
				lResult = lCountInFile;
				break;
			} // if
		} // while
	} // if
	lCountFile.close();
	return lResult;
}

int ha_fakeengine::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_fakeengine::external_lock");
  //printf("ha_fakeengine::external_lock\n");
  DBUG_RETURN(0);
}


THR_LOCK_DATA **ha_fakeengine::store_lock(THD *thd,
                                         THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type)
{
  DBUG_ENTER("ha_fakeengine::store_lock");
  //printf("ha_fakeengine::store_lock\n");
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


int ha_fakeengine::index_read_map(uchar * buf, const uchar * key,
                                 key_part_map keypart_map,
                             enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::index_read_map");
  //printf("ha_fakeengine::index_read_map\n");
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

int ha_fakeengine::index_read_last_map(uchar * buf, const uchar * key,
                                      key_part_map keypart_map)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::index_read_last");
  //printf("ha_fakeengine::index_read_last\n");
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


int ha_fakeengine::index_next(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::index_next");
  //printf("ha_fakeengine::index_next\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}


int ha_fakeengine::index_prev(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::index_prev");
  //printf("ha_fakeengine::index_prev\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}


int ha_fakeengine::index_first(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::index_first");
  //printf("ha_fakeengine::index_first\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}


int ha_fakeengine::index_last(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_fakeengine::index_last");
  //printf("ha_fakeengine::index_last\n");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  table->status= STATUS_NOT_FOUND;
  DBUG_RETURN(rc);
}

static st_fakeengine_share *get_share(const char *table_name)
{
  st_fakeengine_share *share;
  uint length;

  length= (uint) strlen(table_name);
  mysql_mutex_lock(&fakeengine_mutex);
    
  if (!(share= (st_fakeengine_share*)
        my_hash_search(&fakeengine_open_tables,
                       (uchar*) table_name, length)))
  {
    if (!(share= (st_fakeengine_share*) my_malloc(sizeof(st_fakeengine_share) +
                                                 length,
                                                 MYF(MY_WME | MY_ZEROFILL))))
      goto error;

    share->table_name_length= length;
    strmov(share->table_name, table_name);
    
    if (my_hash_insert(&fakeengine_open_tables, (uchar*) share))
    {
      my_free(share);
      share= NULL;
      goto error;
    }
    
    thr_lock_init(&share->lock);
  }
  share->use_count++;
  
error:
  mysql_mutex_unlock(&fakeengine_mutex);
  return share;
}

static void free_share(st_fakeengine_share *share)
{
  mysql_mutex_lock(&fakeengine_mutex);
  if (!--share->use_count)
    my_hash_delete(&fakeengine_open_tables, (uchar*) share);
  mysql_mutex_unlock(&fakeengine_mutex);
}

static void fakeengine_free_key(st_fakeengine_share *share)
{
  thr_lock_delete(&share->lock);
  my_free(share);
}

static uchar* fakeengine_get_key(st_fakeengine_share *share, size_t *length,
                                my_bool not_used __attribute__((unused)))
{
  *length= share->table_name_length;
  return (uchar*) share->table_name;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key bh_key_mutex_fakeengine;

static PSI_mutex_info all_fakeengine_mutexes[]=
{
  { &bh_key_mutex_fakeengine, "blackhole", PSI_FLAG_GLOBAL}
};

void init_ha_fakeengine_psi_keys()
{
  const char* category= "blackhole";
  int count;

  /*if (PSI_server == NULL)
    return;
*/
  count= array_elements(all_fakeengine_mutexes);
  //PSI_server->register_mutex(category, all_blackhole_mutexes, count);
  mysql_mutex_register(category, all_fakeengine_mutexes, count);

}
#endif

static int ha_fakeengine_init(void *p)
{
  //printf("ha_fakeengine_init\n");	
  handlerton *fakeengine_hton;

#ifdef HAVE_PSI_INTERFACE
  init_ha_fakeengine_psi_keys();
#endif

  fakeengine_hton= (handlerton *)p;
  fakeengine_hton->state= SHOW_OPTION_YES;
  fakeengine_hton->db_type= DB_TYPE_BLACKHOLE_DB;
  fakeengine_hton->create= fakeengine_create_handler;
  fakeengine_hton->flags= HTON_CAN_RECREATE;

  //printf("initializes the blackhole\n");
  //CHARSET_INFO *my_system_charset_info = &my_charset_utf8_bin;
  CHARSET_INFO *my_system_charset_info = &my_charset_utf8_general_ci;
  
  mysql_mutex_init(bh_key_mutex_fakeengine,
                   &fakeengine_mutex, MY_MUTEX_INIT_FAST);
  my_hash_init(&fakeengine_open_tables,my_system_charset_info,32,0,0,
                      (my_hash_get_key) fakeengine_get_key,
                      (my_hash_free_key) fakeengine_free_key, 0);

  return 0;
}

static int ha_fakeengine_fini(void *p)
{
  my_hash_free(&fakeengine_open_tables);
  mysql_mutex_destroy(&fakeengine_mutex);

  return 0;
}

struct st_mysql_storage_engine ha_fakeengine_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(ha_fakeengine)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &ha_fakeengine_storage_engine,
  "FAKEENGINE",
  "MySQL AB",
  "/dev/null storage engine (anything you write to it disappears)",
  PLUGIN_LICENSE_GPL,
  ha_fakeengine_init, /* Plugin Init */
  ha_fakeengine_fini, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  NULL,                       /* status variables                */
  NULL,        				  /* system variables                */
  NULL,                       /* config options                  */
  0,                          /* flags                           */
}
mysql_declare_plugin_end;
