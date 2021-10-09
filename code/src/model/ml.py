from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor


# from thundersvm import SVR as gpuSVR


class MLModel:

    @staticmethod
    def available_models():
        # return zip(['LR', 'DT', 'RF', 'SVM'], [MLModel.logistic_regressor, MLModel.dt_regressor, MLModel.rf_regressor, MLModel.svm_regressor])
        # return list(zip(['LR', 'DT', 'RF'], [MLModel.logistic_regressor, MLModel.dt_regressor, MLModel.rf_regressor]))
        return list(
            zip(['Linear', 'LR', 'DT'], [MLModel.linear_regressor, MLModel.logistic_regressor, MLModel.dt_regressor]))

    @staticmethod
    def linear_regressor(X_train, Y_train, X_test):
        # Note LogisticRegression is not for regression but classification!
        # Note The Y variable must be the classification class,
        # Y_train = [1 if n > 0 else 0 for n in Y_train]

        model = LinearRegression()
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)
        # y_pred = [p1 for (p0, p1) in y_pred]
        return model, y_pred, model.coef_

    @staticmethod
    def logistic_regressor(X_train, Y_train, X_test):
        # Note LogisticRegression is not for regression but classification!
        # Note The Y variable must be the classification class,
        Y_train = [1 if n > 0 else 0 for n in Y_train]

        model = LogisticRegression()
        model.fit(X_train, Y_train)
        y_pred = model.predict_proba(X_test)
        y_pred = [p1 for (p0, p1) in y_pred]
        return model, y_pred, model.coef_

    @staticmethod
    def dt_regressor(X_train, Y_train, X_test):
        # max_val = 1.0 * np.max(Y_train)
        # Y_train = [n / max_val for n in Y_train]
        Y_train = [1 if n > 0 else 0 for n in Y_train]

        model = DecisionTreeRegressor()
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)
        return model, y_pred, model.feature_importances_

    @staticmethod
    def rf_regressor(X_train, Y_train, X_test):
        # max_val = 1.0 * np.max(Y_train)
        # Y_train = [n / max_val for n in Y_train]
        Y_train = [1 if n > 0 else 0 for n in Y_train]

        model = RandomForestRegressor()
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)
        return model, y_pred, model.feature_importances_

    @staticmethod
    def svm_regressor(X_train, Y_train, X_test):
        # max_val = 1.0 * np.max(Y_train)
        # Y_train = [n / max_val for n in Y_train]
        Y_train = [1 if n > 0 else 0 for n in Y_train]

        model = SVR(C=1.0, kernel='rbf', degree=3)
        # model = gpuSVR(C=1.0, kernel='rbf', degree=3)
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)
        return model, y_pred, None
