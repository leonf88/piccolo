#ifndef FILEHELPER_H_
#define FILEHELPER_H_

// Converts the recordfile calls into a form accessible from C.

extern "C" {
  struct RecordFile;

  typedef struct {
    int id;
    int num_neighbors;
    int *neighbors;
  } GraphEntry;

  RecordFile *RecordFile_Open(const char* f, const char *mode);
  GraphEntry* RecordFile_ReadGraphEntry(RecordFile *r);

  void RecordFile_Close(RecordFile* f);
}

#endif /* FILEHELPER_H_ */
