from sklearn.svm import SVR

# from thundersvm import SVR as gpuSVR


class MLModel:
    @staticmethod
    def svm_regressor(X_train, Y_train, X_test):
        # max_val = 1.0 * np.max(Y_train)
        # Y_train = [n / max_val for n in Y_train]
        Y_train = [1 if n > 0 else 0 for n in Y_train]

        model = SVR(C=1.0, kernel='rbf', degree=3)
        # model = gpuSVR(C=1.0, kernel='rbf', degree=3)
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)

        # if random.random() < 0.1:
        #     print(len(X_train), len(Y_train), len(X_test))
        #     print(X_train[0], X_test[0])
        #     print('y_pred', y_pred)

        return model, y_pred, None
