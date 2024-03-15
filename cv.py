import pandas as pd
from types import GeneratorType


class NestedCV:
    def __init__(self, k_outer, k_inner):
        self.k_outer = k_outer  # Number of outer folds
        self.k_inner = k_inner  # Number of inner folds

    def split(self, data, date_column):
     data_sorted = data.sort_values(by=date_column)
     total_length = len(data_sorted)
     outer_fold_size = total_length // self.k_outer

    # Outer loop for nested CV
     for outer_i in range(self.k_outer):
        outer_start = outer_i * outer_fold_size
        outer_end = min(outer_start + outer_fold_size, total_length)
        outer_fold_data = data_sorted.iloc[outer_start:outer_end]

        if len(outer_fold_data) < self.k_inner:
            # Base case: If there's not enough data for inner splits, stop recursion
            break

        # Inner CV for walking window
        inner_cv = NestedCV(k_outer=self.k_outer, k_inner=self.k_inner)
        inner_splits = inner_cv.split(outer_fold_data, date_column)

        for inner_train, inner_validate in inner_splits:
            yield outer_fold_data.iloc[:inner_train.index[0]], inner_validate


if __name__ == "__main__":
    # Load dataset using your file path
    data = pd.read_csv("/content/Electric_Production.csv")

    # Assuming "date" is the datetime column and "production" is the target
    date_column = "date"

    # Nested CV parameters
    k_outer = 3  # Number of outer folds
    k_inner = 2  # Number of inner folds (walking window size)

    cv = NestedCV(k_outer, k_inner)
    splits = cv.split(data, date_column)

    # Check return type
    assert isinstance(splits, GeneratorType)

    # Check return types and data leakage
    
    for train, validate in splits:
        
        # types
        assert isinstance(train, pd.DataFrame)
        assert isinstance(validate, pd.DataFrame)

        # shape
        assert train.shape[1] == validate.shape[1]

        # data leak
        assert train["date"].max() <= validate["date"].min()

        count += 1

    # check number of splits returned
    assert count == k
