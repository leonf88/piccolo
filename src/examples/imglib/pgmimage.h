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
 *
 ******************************************************************
 */

#ifndef _PGMIMAGE_H_
#define _PGMIMAGE_H_

#include <iostream>
#include <stdlib.h>
#include <string>
#include "util/common.h"

using namespace std;

class image {
  public:
    image();                              //create empty image
    image(string name, int nr, int nc);   //create with name and size
    image(string filename);               //create from file
    ~image();

    void realloc(string name, int nr, int nc);
    string basename(string filename);
    void setpixel(int r, int c, int val);
    int getpixel(int r, int c);
    int tofile(string filename);

    void corrupt(double sigma);

    int rows() const { return _rows; }
    int cols() const { return _cols; }
    string name() const { return _name; }
    int* raw() const { return data; }

  private:
    string _name;
    int _rows, _cols;
    int *data;
};

class imagelist {
  public:
    imagelist();
    ~imagelist();

    void add(image* img);
    void load_from_infofile(string path, string filename);
    void mungename(char* buf);

    int images() { return n; }
    image* getimage(int m) { return (m<n)?list[m]:NULL; }

  private:
    int n;	//number of images
    image** list;
};

#endif
