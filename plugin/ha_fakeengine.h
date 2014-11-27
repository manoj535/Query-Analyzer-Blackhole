#include "thr_lock.h"                           /* THR_LOCK */
#include "handler.h"                            /* handler */
#include "table.h"                              /* TABLE_SHARE */
#include <string>
#include <fstream>
#include <iostream>
#include <map>
using namespace std;

/*
  Shared structure for correct LOCK operation
*/
struct st_fakeengine_share {
  THR_LOCK lock;
  uint use_count;
  uint table_name_length;
  char table_name[31];
};


/*
  Class definition for the blackhole storage engine
  "Dumbest named feature ever"
*/
class ha_fakeengine: public handler
{
	THR_LOCK_DATA lock;      /* MySQL lock */
	st_fakeengine_share *share;

public:
	ha_fakeengine(handlerton *hton, TABLE_SHARE *table_arg);
	~ha_fakeengine()
	{
	}
	/* The name that will be used for display purposes */
	const char *table_type() const { return "BLACKHOLE"; }
	/*
		The name of the index type that will be used for display
		don't implement this method unless you really have indexes
	*/
	const char *index_type(uint key_number);
	const char **bas_ext() const;
	ulonglong table_flags() const
	{
		return(HA_BLOCK_CONST_TABLE|HA_PRIMARY_KEY_IN_READ_INDEX| HA_NULL_IN_KEY | 
		   HA_CAN_FULLTEXT | HA_CAN_SQL_HANDLER |
           HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE |
           HA_CAN_INDEX_BLOBS | HA_AUTO_PART_KEY | HA_READ_OUT_OF_SYNC |
           HA_FILE_BASED | HA_CAN_GEOMETRY | HA_CAN_INSERT_DELAYED | HA_STATS_RECORDS_IS_EXACT);
	}
	ha_rows records_in_range(uint inx,key_range *min_key,key_range *max_key);
	
	int index_init(uint idx, bool sorted) { active_index= idx; return 0; }
	ulong index_flags(uint inx, uint part, bool all_parts) const
	{
		return (HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE |
            HA_READ_ORDER | HA_KEYREAD_ONLY | HA_DO_INDEX_COND_PUSHDOWN);
	}
	/* The following defines can be increased if necessary */
	#define FAKEENGINE_MAX_KEY	64		/* Max allowed keys */
	#define FAKEENGINE_MAX_KEY_LENGTH 1000
	#define FAKEENGINE_REC_RANGE 3
	uint max_supported_keys()          const { return FAKEENGINE_MAX_KEY; }
	uint max_supported_key_length()    const { return FAKEENGINE_MAX_KEY_LENGTH; }
	uint max_supported_key_part_length() const { return FAKEENGINE_MAX_KEY_LENGTH; }
	int open(const char *name, int mode, uint test_if_locked);
	int close(void);
	int truncate();
	int rnd_init(bool scan);
	int rnd_next(uchar *buf);
	int rnd_pos(uchar * buf, uchar *pos);
	int index_read_map(uchar * buf, const uchar * key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag);
	/*int index_read_idx_map(uchar * buf, uint idx, const uchar * key,
                         key_part_map keypart_map,
                         enum ha_rkey_function find_flag);*/                     
	int index_read_last_map(uchar * buf, const uchar * key, key_part_map keypart_map);
	int index_next(uchar * buf);
	int index_prev(uchar * buf);
	int index_first(uchar * buf);
	int index_last(uchar * buf);
	void position(const uchar *record);
	int info(uint flag);
	int external_lock(THD *thd, int lock_type);
	int create(const char *name, TABLE *table_arg,
             HA_CREATE_INFO *create_info);
	THR_LOCK_DATA **store_lock(THD *thd,
                             THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);
     /*ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                     void *seq_init_param, uint n_ranges_arg,
                                     uint *bufsz, uint *flags, 
                                     Cost_estimate *cost);*/
     //double read_time(uint index, uint ranges, ha_rows rows);
     //double scan_time();
     //double index_only_read_time(uint keynr, double records);
private:
	int mDefaultRowCount; // Default row count if not given config file
	std::string mTableName;
	const string kRowCountFilePath;
	const string kIndexCardinalityFilePath;
	const string kIndexRangeFilePath;
	long getIndexCount(string iFilePath, string iIndexName);
	//map<std::pair<std::string,std::string>, int> mIndexMap;
	virtual int write_row(uchar *buf);
	virtual int update_row(const uchar *old_data, uchar *new_data);
	virtual int delete_row(const uchar *buf);
};
