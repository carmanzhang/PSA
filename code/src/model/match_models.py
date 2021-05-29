import matchzoo as mz


def get_model(algo, args, vocab_size):
    algo = algo + '_'
    if len(args) != 0:
        if algo == 'dssm_':
            model = mz.models.DSSM()
            model.params['with_multi_layer_perceptron'] = True
            model.params['mlp_num_layers'] = args[algo + 'layers']
            model.params['mlp_num_units'] = args[algo + 'units']
            model.params['mlp_num_fan_out'] = 128
            model.params['mlp_activation_func'] = 'relu'
        elif algo == 'convknrm_':
            model = mz.models.ConvKNRM()
            model.params['with_embedding'] = True
            model.params['embedding_input_dim'] = vocab_size  # Usually equals vocab size + 1. Should be set manually.
            model.params['embedding_output_dim'] = args[algo + 'ebd_out_dim']  # Should be set manually.
            model.params['embedding_trainable'] = True
            model.params['filters'] = args[algo + 'filter']  # The filter size in the convolution layer.
            model.params['conv_activation_func'] = 'relu'  # The activation function in the convolution layer.
            model.params['max_ngram'] = args[algo + 'ngram']  # The maximum length of n-grams for the convolution layer.
            model.params[
                'use_crossmatch'] = True  # Whether to match left n-grams and right n-grams of different lengths
            model.params[
                'kernel_num'] = args[
                algo + 'kernels']  # The number of RBF kernels.	 quantitative uniform distribution in [5, 20), with a step size of 1
            model.params['sigma'] = 0.1  # quantitative uniform distribution in [0.01, 0.2), with a step size of 0.01
            model.params['exact_sigma'] = 0.001  # The exact_sigma denotes the sigma for exact match.
        elif algo == 'anmm_':
            model = mz.models.ANMM()
            model.params['with_embedding'] = True
            model.params['embedding_input_dim'] = vocab_size  # Usually equals vocab size + 1. Should be set manually.
            model.params['embedding_output_dim'] = args[algo + 'ebd_out_dim']  # Should be set manually.
            model.params['embedding_trainable'] = True
            model.params[
                'dropout_rate'] = args[
                algo + 'dropout']  # The dropout rate. quantitative uniform distribution in [0, 1), with a step size of 0.05
            model.params['num_layers'] = args[algo + 'layers']  # Number of hidden layers in the MLP layer.
            model.params['hidden_sizes'] = [args[algo + 'units'],
                                            args[algo + 'units']]  # Number of hidden size for each hidden layer
        elif algo == 'hbmp_':
            model = mz.contrib.models.HBMP()
            model.params['embedding_input_dim'] = vocab_size
            model.params['embedding_output_dim'] = args[algo + 'ebd_out_dim']
            model.params['embedding_trainable'] = True
            model.params['alpha'] = args[algo + 'alpha']
            model.params['mlp_num_layers'] = args[algo + 'layers']
            model.params['mlp_num_units'] = [args[algo + 'units'], args[algo + 'units']]
            model.params['lstm_num_units'] = args[algo + 'lstm_units']
            model.params['dropout_rate'] = args[algo + 'dropout']
    else:
        if algo == 'dssm_':
            model = mz.models.DSSM()
            model.params['with_multi_layer_perceptron'] = True
            model.params['mlp_num_layers'] = 4
            model.params['mlp_num_units'] = 128
            model.params['mlp_num_fan_out'] = 128
            model.params['mlp_activation_func'] = 'relu'
        elif algo == 'convknrm_':
            model = mz.models.ConvKNRM()
            model.params['with_embedding'] = True
            model.params['embedding_input_dim'] = vocab_size  # Usually equals vocab size + 1. Should be set manually.
            model.params['embedding_output_dim'] = 300  # Should be set manually.
            model.params['embedding_trainable'] = True
            model.params['filters'] = 128  # The filter size in the convolution layer.
            model.params['conv_activation_func'] = 'relu'  # The activation function in the convolution layer.
            model.params['max_ngram'] = 3  # The maximum length of n-grams for the convolution layer.
            model.params[
                'use_crossmatch'] = True  # Whether to match left n-grams and right n-grams of different lengths
            model.params[
                'kernel_num'] = 11  # The number of RBF kernels.	 quantitative uniform distribution in [5, 20), with a step size of 1
            model.params['sigma'] = 0.1  # quantitative uniform distribution in [0.01, 0.2), with a step size of 0.01
            model.params['exact_sigma'] = 0.001  # The exact_sigma denotes the sigma for exact match.
        elif algo == 'anmm_':
            model = mz.models.ANMM()
            model.params['with_embedding'] = True
            model.params['embedding_input_dim'] = vocab_size  # Usually equals vocab size + 1. Should be set manually.
            model.params['embedding_output_dim'] = 300  # Should be set manually.
            model.params['embedding_trainable'] = True
            model.params[
                'dropout_rate'] = 0.1  # The dropout rate. quantitative uniform distribution in [0, 1), with a step size of 0.05
            model.params['num_layers'] = 2  # Number of hidden layers in the MLP layer.
            model.params['hidden_sizes'] = [30, 30]  # Number of hidden size for each hidden layer
        elif algo == 'hbmp_':
            model = mz.contrib.models.HBMP()
            model.params['embedding_input_dim'] = vocab_size
            model.params['embedding_output_dim'] = 100
            model.params['embedding_trainable'] = True
            model.params['alpha'] = 0.1
            model.params['mlp_num_layers'] = 3
            model.params['mlp_num_units'] = [10, 10]
            model.params['lstm_num_units'] = 5
            model.params['dropout_rate'] = 0.1

    return model
