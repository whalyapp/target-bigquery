import io
import sys
import logging
import json
import simplejson
import logging
import singer

RAW_LINE_SIZE = '__raw_line_size'

logger = singer.get_logger()

def get_line_size(line_data):
    return line_data.get(RAW_LINE_SIZE) or len(json.dumps(line_data))

class BufferedSingerStream():
    def __init__(self,
                 bigquery_client,
                 *args,
                 max_rows=10000,
                 max_buffer_size=52428800,  # 50MB
                 **kwargs):
        
        self.stream = ""
        self.bigquery_client = bigquery_client
        self.max_rows = max_rows
        self.max_buffer_size = max_buffer_size

        self.__buffer = []
        self.__count = 0
        self.__size = 0

    @property
    def count(self):
        return self.__count

    @property
    def buffer_full(self):
        if self.__count >= self.max_rows:
            return True

        if self.__count > 0:
            if self.__size >= self.max_buffer_size:
                return True

        return False


    def add_record_message(self, stream, record_message):
        previous_stream = self.stream
        # logger.info("recieving for stream {} the following object {}".format(record_message, stream))
        logger.info("buffer size {} - is buffer full {}".format(self.count, self.buffer_full))
        if previous_stream != stream or self.buffer_full:
            logger.info("switching stream from {} to {}".format(previous_stream, stream))
            self.flush_buffer()
            self.stream = stream

        self.__buffer.append(record_message)
        self.__size += get_line_size(record_message)
        self.__count += 1

    def peek_buffer(self):
        return self.__buffer

    def flush_buffer(self):
        _buffer = self.__buffer
        logger.info("flushing buffer")
        self.__buffer = []
        self.__size = 0
        self.__count = 0
        if (len(_buffer)):
            logger.info("executing insert with {} records".format(len(_buffer)))
            err = self.bigquery_client.insert_rows_json(self.stream, _buffer, ignore_unknown_values=True, skip_invalid_rows=False)
            if len(err):
                logger.error("Error syncing objects to {} got the following errors {}".format(self.stream, err))
