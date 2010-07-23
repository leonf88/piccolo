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
DEFINE_int32(hidden_neurons, 16, "Number of hidden heurons");
DEFINE_int32(savedelta, 100, "Save net every (savedelta) epochs");
DEFINE_int32(sharding, 100, "Images per kernel execution");
DEFINE_bool(list_errors, false, "If true, will ennumerate misclassed images");
DEFINE_int32(total_ims, 43115, "Total number of images in DB");
DEFINE_int32(im_x, 32, "Image width in pixels");
DEFINE_int32(im_y, 32, "Image height in pixels");
DEFINE_bool(verify, true, "If true, will check initial data put into tables for veracity");
DEFINE_double(eta, 0.3, "Learning rate of BPNN");
DEFINE_double(momentum, 0.3, "Momentum of BPNN");

static TypedGlobalTable<int, double>* nn_weights  = NULL;
static TypedGlobalTable<int, double>* nn_biases   = NULL;
static TypedGlobalTable<int, IMAGE>*  train_ims   = NULL;
static TypedGlobalTable<int, double>* performance = NULL;

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
		BackProp bpnn;
		ImageNet imnet;
		int hiddenn,imgsize;
		double out_err, hid_err, sumerr;

		void InitKernel() {
			imgsize = FLAGS_im_x*FLAGS_im_y;
			hiddenn = FLAGS_hidden_neurons;
			net = bpnn.bpnn_create(imgsize, hiddenn, 1);
		}

		void Initialize() {
			string netname, pathpn, infopn;
			IMAGELIST *trainlist;
			IMAGE *iimg;
			int ind, seed, savedelta, list_errors;
			int train_n, i, j;

			//defaults
			seed = 20100630;
			savedelta = FLAGS_savedelta;
			list_errors = FLAGS_list_errors;
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
			int i, j, k;

			if (0 > TableToBPNN(net)) {
				fprintf(stderr,"Fatal error: could not load bpnn from table\n");
				exit(-1);
			}

			TypedTableIterator<int, IMAGE> *it = train_ims->get_typed_iterator(current_shard());
			for (; !it->done(); it->Next()) {
				IMAGE thisimg = it->value();						//grab this image

				imnet.load_input_with_image(&thisimg, net);				//load the input layer
				imnet.load_target(&thisimg, net);					//load target output layer

				/*** Feed forward input activations. ***/
				bpnn.bpnn_layerforward(net->input_units, net->hidden_units,
						net->input_weights, imgsize, hiddenn);
				bpnn.bpnn_layerforward(net->hidden_units, net->output_units,
						net->hidden_weights, hiddenn, 1);

				/*** Compute error on output and hidden units. ***/
				bpnn.bpnn_output_error(net->output_delta, net->target, net->output_units,
						1, &out_err);
				bpnn.bpnn_hidden_error(net->hidden_delta, hiddenn, net->output_delta, 1,
						net->hidden_weights, net->hidden_units, &hid_err);

				/*** Adjust input and hidden weights in database. ***/
				UpdateBPNNWithDeltas(net->output_delta, 1, net->hidden_units, hiddenn,
						net->hidden_weights, net->hidden_prev_weights, FLAGS_eta, FLAGS_momentum,((imgsize+1)*(hiddenn+1)));
				UpdateBPNNWithDeltas(net->hidden_delta, hiddenn, net->input_units, imgsize,
						net->input_weights, net->input_prev_weights, FLAGS_eta, FLAGS_momentum,0);

				/*** Adjust input and hidden weights. ***/
				bpnn.bpnn_adjust_weights(net->output_delta, 1, net->hidden_units, hiddenn,
						net->hidden_weights, net->hidden_prev_weights, FLAGS_eta, FLAGS_momentum);
				bpnn.bpnn_adjust_weights(net->hidden_delta, hiddenn, net->input_units, imgsize,
						net->input_weights, net->input_prev_weights, FLAGS_eta, FLAGS_momentum);

			}

		}

		void PerformanceCheck() {
			double delta,err;
			bool classed;										//true if this image was correct classed

			if (0 > TableToBPNN(net)) {
				fprintf(stderr,"Fatal error: could not load bpnn from table\n");
				exit(-1);
			}

			TypedTableIterator<int, IMAGE> *it = train_ims->get_typed_iterator(current_shard());
			for (; !it->done(); it->Next()) {
				IMAGE thisimg = it->value();						//grab this image

				imnet.load_input_with_image(&thisimg, net);			//load the input layer
				bpnn.bpnn_feedforward(net);
				imnet.load_target(&thisimg, net);					//load target output layer
				delta = net->target[1] - net->output_units[1];
				err   = 0.5*delta*delta;							//.5delta^2
				performance->update(1,err);							//accumulate more error

				classed = (net->output_units[1] > 0.5);
				classed = (net->target[1] > 0.5)?classed:!classed;
				performance->update(0,classed?1:0);					//accumulate correct classifications
			}
		}

		void DisplayPerformance() {

			double ims_correct = performance->get(0);
			double total_err   = performance->get(1);
			printf("Performance: %d of %d (%.0f%%) images correctly classified, average err %f\n",
					(int)ims_correct,
					FLAGS_total_ims,
					round(100*(ims_correct/((double)FLAGS_total_ims))),
					(total_err/((double)FLAGS_total_ims))
				  );
			performance->update(0,-ims_correct);
			performance->update(1,-total_err);
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

		int UpdateBPNNWithDeltas(double *delta, int ndelta, double *ly, int nly, double **w,
				double **oldw, double eta, double momentum,int idx_offset) {
			double newval_dw;
			int k, j;
			ly[0] = 1.0;
			for (j = 1; j <= ndelta; j++) {
				for (k = 0; k <= nly; k++) {
					newval_dw = ((eta * delta[j] * ly[k]) + (momentum * oldw[k][j]));
					nn_weights->update(idx_offset+(k*(ndelta+1))+j,newval_dw);
				}
			}
		}


};
REGISTER_KERNEL(FCKernel);
REGISTER_METHOD(FCKernel, InitKernel);
REGISTER_METHOD(FCKernel, Initialize);
REGISTER_METHOD(FCKernel, TrainIteration);
REGISTER_METHOD(FCKernel, PerformanceCheck);
REGISTER_METHOD(FCKernel, DisplayPerformance);

int Faceclass(ConfigData& conf) {

	int i,j;

	nn_weights  = CreateTable(0,1,new Sharding::Mod,new Accumulators<double>::Sum);
	nn_biases   = CreateTable(1,1,new Sharding::Mod,new Accumulators<double>::Sum);
	train_ims   = CreateTable(2,ceil(FLAGS_total_ims/FLAGS_sharding),new Sharding::Mod, new Accumulators<IMAGE>::Replace);
	performance = CreateTable(3,1,new Sharding::Mod, new Accumulators<double>::Sum);

	StartWorker(conf);
	Master m(conf);


	NUM_WORKERS = conf.num_workers();
	printf("---- Initializing FaceClass on %d workers ----\n",NUM_WORKERS);

	//m.run_all("FCKernel","InitKernel");
	m.run_one("FCKernel","Initialize",nn_weights);

	if (FLAGS_epochs > 0) {
		printf("Training underway (going to %d epochs)\n", FLAGS_epochs);
		printf("Will save network every %d epochs\n", FLAGS_savedelta);
		fflush(stdout);
	}

	for(i=0;i<FLAGS_epochs;i++) {
		printf("--- Running epoch %03d of %03d ---\n",i,FLAGS_epochs);
		m.run_all("FCKernel","TrainIteration",train_ims);
		m.run_all("FCKernel","PerformanceCheck",train_ims);
		m.run_one("FCKernel","DisplayPerformance",performance);
	}

	return 0;
}
REGISTER_RUNNER(Faceclass);
