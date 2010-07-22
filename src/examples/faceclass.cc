#include "client/client.h"
#include "examples/examples.pb.h"

extern "C" {
#include "facedet/cpp/pgmimage.h"
}
#include "facedet/cpp/backprop.hpp"
#include "facedet/cpp/imagenet.hpp"

#include <sys/time.h>
#include <sys/resource.h>
#include <algorithm>
#include <libgen.h>

using namespace dsm;
using namespace std;

static int NUM_WORKERS = 2;

DEFINE_string(infopn, "trainpn_random.info", "File containing list of training images");
DEFINE_string(netname, "/home/kerm/piccolo/src/examples/facedet/netsave.net", "Filename of neural network save file");
DEFINE_string(pathpn, "/home/kerm/piccolo/src/examples/facedet/trainset/", "Path to training data");
DEFINE_int32(epochs, 100, "Number of training epochs");
DEFINE_int32(hidden_neurons, 256, "Number of hidden heurons");
DEFINE_int32(savedelta, 100, "Save net every (savedelta) epochs");
DEFINE_int32(sharding, 100, "Images per kernel execution");
DEFINE_bool(list_errors, false, "If true, will ennumerate misclassed images");
DEFINE_int32(total_ims, 43115, "Total number of images in DB");
DEFINE_int32(im_x, 32, "Image width in pixels");
DEFINE_int32(im_y, 32, "Image height in pixels");
DEFINE_bool(verify, true, "If true, will check initial data put into tables for veracity");

static TypedGlobalTable<int, double>* nn_weights = NULL;
static TypedGlobalTable<int, double>* nn_biases  = NULL;
static TypedGlobalTable<int, IMAGE>*  train_ims  = NULL;

//-----------------------------------------------
// Marshalling for IMAGE* type
//-----------------------------------------------
namespace dsm {
	template <> struct Marshal<IMAGE> {
		static void marshal(const IMAGE& t, string *out) {
			char sizes[4];
			sizes[0] = (char)((t.rows)/256);
			sizes[1] = (char)((t.rows)%256);
			sizes[2] = (char)((t.cols)/256);
			sizes[3] = (char)((t.cols)%256);
			out->append(sizes,4);
			out->append((char*)(t.data),sizeof(int)*(t.rows)*(t.cols));
			sizes[0] = (char)(((strlen(t.name))>256)?256:(strlen(t.name)));
			out->append(sizes,1);
			out->append((char*)(t.name),(int)sizes[0]);
		}
		static void unmarshal(const StringPiece &s, IMAGE* t) {
			int r,c,sl;
			r = (256*(unsigned int)(s.data[0])) + (unsigned int)(s.data[1]);
			c = (256*(unsigned int)(s.data[2])) + (unsigned int)(s.data[3]);
			t->rows = r;
			t->cols = c;
			if (NULL == (t->data = (int*)malloc(sizeof(int)*r*c))) {
				fprintf(stderr,"Failed to marshal an image: out of memory.\n");
				exit(-1);
			}
			memcpy(t->data,s.data+4,sizeof(int)*r*c);
			sl = (unsigned int)(s.data[4+sizeof(int)*r*c]);
			if (NULL == (t->name = (char*)malloc(sl+1))) {
				fprintf(stderr,"Failed to marshal an image: out of memory.\n");
				exit(-1);
			}
			strncpy(t->name,s.data+5+sizeof(int)*r*c,sl);
			t->name[sl] = '\0';
		}
	};
}

//----------------------------------------------------
// Face Classifier Kernel
// Takes a set of face samples, trains against it,
// then writes back a set of delta weights and biases
// from the oiginal model.
//----------------------------------------------------

class FCKernel : public DSMKernel {
	public:
		int iter;
		BPNN *net;
		int hiddenn,imgsize;

		void InitKernel() {
		}

		void Initialize() {
			string netname, pathpn, infopn;
			IMAGELIST *trainlist;
			IMAGE *iimg;
			int ind, seed, savedelta, list_errors;
			int train_n, i, j;
			double out_err, hid_err, sumerr;
			BackProp bpnn;

			//defaults
			seed = 20100630;
			savedelta = FLAGS_savedelta;
			list_errors = FLAGS_list_errors;
			hiddenn = FLAGS_hidden_neurons;
			netname = FLAGS_netname;
			pathpn = FLAGS_pathpn;
			infopn = FLAGS_infopn;

			/*** Create imagelists ***/
			trainlist = imgl_alloc();

			/*** Don't try to train if there's no training data ***/
			if (infopn.length() == 0 || pathpn.length() == 0) {
				printf("FaceClass: Must specify path and filename of training data\n");
				exit(-1);
			}

			/*** Loading training images ***/
			imgl_load_images_from_infofile(trainlist, pathpn.c_str(), infopn.c_str());

			/*** If we haven't specified a network save file, we should... ***/
			if (netname.length() == 0) {
				printf("Faceclass: Must specify an output file\n");
				exit(-1);
			}

			/*** Initialize the neural net package ***/
			bpnn.bpnn_initialize(seed);

			/*** Show number of images in train, test1, test2 ***/
			printf("%d images in training set\n", trainlist->n);

			/*** If we've got at least one image to train on, go train the net ***/
			train_n = trainlist->n;
			if (train_n <= 0) {
				printf("FaceClass: Must have at least one image to train from\n");
				exit(-1);
			}

			/*** Turn the IMAGELIST into a database of images, ie,  TypedGlobalTable<int, IMAGE>* train_ims ***/

			for(i=0;i<trainlist->n;i++) {
				train_ims->update(i,*(trainlist->list[i]));
			}

			/*** If requested, grab all the images out again and make sure they're correct ***/
			if(FLAGS_verify == true) {
				int goodims = 0;
				for(i=0;i<trainlist->n;i++) {
					IMAGE testim = train_ims->get(i);
					if (testim.rows == (trainlist->list[i])->rows &&
							testim.cols == (trainlist->list[i])->cols) {
						if (!strcmp(testim.name,(trainlist->list[i])->name)) {
							bool immatch = true;
							for(j=0;j<(testim.rows*testim.cols);j++) {
								if (testim.data[j] != (trainlist->list[i])->data[j]) {
									immatch = false;
									break;
								}
							}
							if (!immatch) {
								fprintf(stderr,"[Verify] Image %d did not match image data in DB\n",i);
							} else {
								goodims++;
							}
						} else {
							fprintf(stderr,"[Verify] Image %d did not match image name in DB\n",i);
						}
					} else {
						fprintf(stderr,"[Verify] Image %d has incorrect dimensions in DB\n",i);
					}
				}
				printf("[Verify] %d of %d images matched correctly in the DB\n",goodims,trainlist->n);
			}

			/*** Read network in if it exists, otherwise make one from scratch ***/
			if ((net = bpnn.bpnn_read(netname.c_str())) == NULL) {
				printf("Creating new network '%s'\n", netname.c_str());
				iimg = trainlist->list[0];
				imgsize = ROWS(iimg) * COLS(iimg);
				/* bthom ===========================
				   make a net with:
				   imgsize inputs, N hidden units, and 1 output unit
				 */
				net = bpnn.bpnn_create(imgsize, hiddenn, 1);
			}

			/*** Put initial network into database ***/
			for(i=0;i<imgsize+1;i++) {							// Input layer biases
				nn_biases->update(i,net->input_units[i]);
			}
			for(i=0;i<hiddenn+1;i++) {							// Hidden layer biases
				nn_biases->update(i+(imgsize+1),net->hidden_units[i]);
			}
			for(i=0;i<1+1;i++) {								// Output layer biases
				nn_biases->update(i+(imgsize+1)+(hiddenn+1),net->output_units[i]);
			}
			for(i=0;i<(imgsize+1);i++) {						// Input->Hidden weights
				for(j=0;j<(hiddenn+1);j++) {
					nn_weights->update((i*(hiddenn+1))+j,net->input_weights[i][j]);
				}
			}
			for(i=0;i<(hiddenn+1);i++) {						//Hidden->Output weights
				for(j=0;j<(1+1);j++) {
					nn_weights->update((((imgsize+1)*(hiddenn+1))+(i*(1+1))+j),net->hidden_weights[i][j]);
				}
			}
			printf("NN biases and weights stored in tables\n");

			/*** If requested, grab all the weights/biases out again and make sure they're correct ***/
			if(FLAGS_verify == true) {
				for(i=0;i<imgsize+1;i++) {							// Input layer biases
					if (net->input_units[i] != nn_biases->get(i)) {
						fprintf(stderr,"[Verify] NN bias (input,%d) did not match\n",i);
					}
				}
				for(i=0;i<hiddenn+1;i++) {							// Hidden layer biases
					if (net->hidden_units[i] != nn_biases->get(i+(imgsize+1))) {
						fprintf(stderr,"[Verify] NN bias (hidden,%d) did not match\n",i);
					}
				}
				for(i=0;i<1+1;i++) {								// Output layer biases
					if (net->output_units[i] != nn_biases->get(i+(imgsize+1)+(hiddenn+1))) {
						fprintf(stderr,"[Verify] NN bias (output,%d) did not match\n",i);
					}
				}
				for(i=0;i<(imgsize+1);i++) {						// Input->Hidden weights
					for(j=0;j<(hiddenn+1);j++) {
						if (net->input_weights[i][j] != nn_weights->get((i*(hiddenn+1))+j)) {
							fprintf(stderr,"[Verify] NN weight (input,%d,%d) did not match\n",i,j);
						}
					}
				}
				for(i=0;i<(hiddenn+1);i++) {						//Hidden->Output weights
					for(j=0;j<(1+1);j++) {
						if (net->hidden_weights[i][j] != nn_weights->get((((imgsize+1)*(hiddenn+1))+(i*(1+1))+j))) {
							fprintf(stderr,"[Verify] NN weight (hidden,%d,%d) did not match\n",i,j);
						}
					}
				}

			}
		}

		void TrainIteration() {
			if (0 > TableToBPNN(net)) {
				fprintf(stderr,"Fatal error: could not load bpnn from table\n");
				exit(-1);
			}
			//training iterations
			//grab image
			//train on it
			//update deltas
		}

		void WriteStatus() {
		}

	private:
		int TableToBPNN(BPNN* thisnet) {
			int i,j;

			for(i=0;i<imgsize+1;i++) {							// Input layer biases
				net->input_units[i] = nn_biases->get(i);
			}
			for(i=0;i<hiddenn+1;i++) {							// Hidden layer biases
				net->hidden_units[i] = nn_biases->get(i+(imgsize+1));
			}
			for(i=0;i<1+1;i++) {								// Output layer biases
				net->output_units[i] = nn_biases->get(i+(imgsize+1)+(hiddenn+1));
			}
			for(i=0;i<(imgsize+1);i++) {						// Input->Hidden weights
				for(j=0;j<(hiddenn+1);j++) {
					net->input_weights[i][j] = nn_weights->get((i*(hiddenn+1))+j);
				}
			}
			for(i=0;i<(hiddenn+1);i++) {						//Hidden->Output weights
				for(j=0;j<(1+1);j++) {
					net->hidden_weights[i][j] = nn_weights->get((((imgsize+1)*(hiddenn+1))+(i*(1+1))+j));
				}
			}
			return 0;			//success
		}

};
REGISTER_KERNEL(FCKernel);
REGISTER_METHOD(FCKernel, Initialize);
REGISTER_METHOD(FCKernel, TrainIteration);

int Faceclass(ConfigData& conf) {

	int i,j;

	nn_weights = CreateTable(0,1,new Sharding::Mod,new Accumulators<double>::Sum);
	nn_biases  = CreateTable(1,1,new Sharding::Mod,new Accumulators<double>::Sum);
	train_ims  = CreateTable(2,ceil(FLAGS_total_ims/FLAGS_sharding),new Sharding::Mod, new Accumulators<IMAGE>::Replace);

	StartWorker(conf);
	Master m(conf);


	//NUM_WORKERS = conf.num_workers();

	m.run_one("FCKernel","Initialize",nn_weights);

	if (FLAGS_epochs > 0) {
		printf("Training underway (going to %d epochs)\n", FLAGS_epochs);
		printf("Will save network every %d epochs\n", FLAGS_savedelta);
		fflush(stdout);
	}

	for(i=0;i<FLAGS_epochs;i++) {
		printf("--- Running epoch %03d of %03d ---\n",i,FLAGS_epochs);
		m.run_all("FCKernel","TrainIteration",train_ims);
	}

	return 0;
}
REGISTER_RUNNER(Faceclass);
