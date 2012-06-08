/*
 ******************************************************************
 * HISTORY
 * 15-Oct-94   Jeff Shufelt (js), Carnegie Mellon University
 *      Prepared for 15-681, Fall 1994.
 *
 * 29-June-10  Christopher Mitchell, Courant Institute
 *      Modified for use with Piccolo project
 *      For use with train.info format from Scouter project
 *
 * 26-April-12 Christopher Mitchell, Courant Institute
 *      Properly C++-ified for use with image-denoising application
 ******************************************************************
 */

#include <string>
#include <cstdio>
#include "pgmimage.h"

image::image() {
  _name = "";
  data = NULL;
  _rows = _cols = 0;
}

image::image(string name, int nr, int nc) {
  data = (int *)malloc((unsigned (nr * nc * sizeof(int))));
  CHECK_NE(data, static_cast<int*>(NULL)) << "Failed to reserve memory for image!";
  _rows = nr;
  _cols = nc;
  _name = basename(name);
  for(int i=0; i<nr; i++) {
    for(int j=0; j<nc; j++) {
      setpixel(i, j, 0);
    }
  }
}

image::image(const image& otherim) {
  data = (int*)malloc(unsigned (otherim.rows() * otherim.cols() *
                      sizeof(int)));
  CHECK_NE(data, static_cast<int*>(NULL)) << "Failed to reserve memory for image!";
  _rows = otherim.rows();
  _cols = otherim.cols();
  _name = otherim.name();
  memcpy((void*)data,(void*)otherim.raw(),sizeof(int)*_rows*_cols); 
}

void image::realloc(string name, int nr, int nc) {
  if (data && _rows && _cols)
    free(data);
  data = NULL;
  _rows = _cols = 0;
  image(name,nr,nc);
}

image::image(string filename) {
  FILE *pgm;
  char line[512], intbuf[100], ch;
  int type, nc, nr, maxval;

  pgm = fopen(filename.c_str(), "r");
  CHECK_NE(pgm,static_cast<FILE*>(NULL)) << "Failed to open PGM image " << filename;

  _name = basename(filename);

  /*** Scan pnm type information, expecting P5 ***/
  char* result = fgets(line, 511, pgm);
  CHECK_NE(result,static_cast<char*>(NULL)) << 
          "Failed to read PGM header from " << filename;

  if (1 == sscanf(line, "P%d %d %d %d", &type, &nc, &nr, &maxval)) {
    result = fgets(line, 511, pgm);
    if (2 == sscanf(line, "%d %d %d", &nc, &nr, &maxval)) {
      result = fgets(line, 511, pgm);
      sscanf(line, "%d", &maxval);
    }
  }

  if (type != 5 && type != 2) {
    LOG(FATAL) << "Can only open PGM files (type P5 or P2)";
  }

  /*** Get dimensions of pgm ***/
  _rows = nr;
  _cols = nc;

  /*** Get maxval ***/
  CHECK_LE(maxval,255) << "Can only handle 8-bit (or shallower) PGM images";

  data = (int *) malloc ((unsigned) (nr * nc * sizeof(int)));
  CHECK_NE(data,static_cast<int*>(NULL)) << "Failed to reserve memory for image!";

  if (type == 5) {

    for (int i = 0; i < nr; i++) {
      for (int j = 0; j < nc; j++) {
        setpixel(i, j, fgetc(pgm));
      }
    }

  } else if (type == 2) {

    for (int i=0; i<nr; i++) {
      for (int j=0; j<nc; j++) {

        int k=0, found=0;
        while (!found) {
          ch = (char) fgetc(pgm);
          if (ch >= '0' && ch <= '9') {
            intbuf[k] = ch;  k++;
  	      } else {
            if (k != 0) {
              intbuf[k] = '\0';
              found = 1;
	        }
	      }
        }
        setpixel(i, j, atoi(intbuf));
      }
    }

  } else {
    LOG(FATAL) << "Fatal impossible error (?)";
  }

  fclose(pgm);
}

image::~image() {
  if (data && _rows && _cols)
    free(data);
  data = NULL;
}

string image::basename(string filename)
{
  size_t idx = filename.rfind('/');
  if (idx == filename.npos)
    return filename;
  return filename.substr(idx+1);
}

int image::tofile(string filename) {
  int k, val;
  FILE *iop;

  iop = fopen(filename.c_str(), "w");
  CHECK_NE(iop,static_cast<FILE*>(NULL)) << "Failed to open " << filename << " for writing";
  fprintf(iop, "P2\n");
  fprintf(iop, "%d %d\n", _cols, _rows);
  fprintf(iop, "255\n");

  k=1;
  for (int i=0; i<_rows; i++) {
    for (int j=0; j<_cols; j++) {
      val = getpixel(i, j);
      if ((val < 0) || (val > 255)) {
        LOG(WARNING) << "Found value " << val << " at row " << i << ", col " <<
                     j << ", setting to zero";
        val=0;
      }
      fprintf(iop, "%d%c", val, (k%10)?' ':'\n');
      k++;
    }
  }
  fprintf(iop, "\n");
  fclose(iop);
  return 1;
}

void image::corrupt(float sigma) {
  boost::lagged_fibonacci607 rng;
  boost::normal_distribution<float> noise_model(0, sigma);
  for(int i=0; i<_rows; i++) {
    for(int j=0; j<_cols; j++) {
       int val = getpixel(i,j) + 255*noise_model(rng);
       val = (val > MAX_PXL_VAL)?MAX_PXL_VAL:((val < MIN_PXL_VAL)?MIN_PXL_VAL:val);
       setpixel(i,j,val);
    }
  }
}

double image::calcMSEfrom(image otherim) {
  long long unsigned int sumerr = 0;
  for(int i=0; i<_rows; i++) {
    for(int j=0; j<_cols; j++) {
      sumerr += (getpixel(i,j) - otherim.getpixel(i,j))*
                (getpixel(i,j) - otherim.getpixel(i,j));
    }
  }
  return (double)sumerr/(double)(_rows*_cols);
}

imagelist::imagelist() {
  n = 0;
  list = NULL;
}

imagelist::~imagelist() {
  if (list)
    free(list);
}
  
void imagelist::add(image *img)
{
  if (n == 0) {
    list = (image **) malloc ((unsigned)(sizeof(image*)));
  } else {
    list = (image **) realloc ((char*)list,
      (unsigned) ((n+1) * sizeof (image*)));
  }

  CHECK_NE(list,static_cast<image**>(NULL)) << "Couldn't [re]allocate image list.";
  list[n++] = img;
}

void imagelist::load_from_infofile(string path, string filename) {
  FILE* fp;
  char buf[2000];
  char concatpath[2048];

  CHECK_GT(filename.length(),0) << "Invalid file '" << filename << "'";
  CHECK_GT(path.length(),0) << "Invalid path '" << path << "'";

  strcpy(concatpath,path.c_str());
  if (path[path.length()-1] != '/')
    strcat(concatpath,"/");
  strcat(concatpath,filename.c_str());

  fp = fopen(concatpath, "r");
  CHECK_NE(fp,static_cast<FILE*>(NULL)) << "Could not open '" << concatpath << "'";

  while (fgets(buf, 1999, fp) != NULL) {

    mungename(buf);

    //force the path to end after the slash
    concatpath[path.length()+(int)(path[path.length()-1] != '/')] = '\0';

    strcat(concatpath,buf);
    string tempfull(concatpath);
    image* iimg = new image(tempfull);
    add(iimg);
    fflush(stdout);
  }

  fclose(fp);
}


// CM: Function modified to split on tab char as well
//     as newline, as per train.info format.
void imagelist::mungename(char *buf)
{
  int j;

  j = 0;
  while (buf[j] != '\n' && buf[j] != '\t' ) j++;
  buf[j] = '\0';
}
