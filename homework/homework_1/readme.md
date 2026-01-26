# Homework 1
```
Nathaniel Stall
alt5629
```

## Instructions to Run
1. `pip install` the following if you haven't already:
    - `pandas`
    - `matplotlib`
    - `missingno`
    - `seaborn`
    - `scipy`
2. Execute the cells in `DATA_ENG300_HW1_Aircraft-1.ipynb` in sequential order. In VSCode, you can do this with the "Run All" button at the top of the screen.
3. A variety of dataframes, outputs, and plots will populate in the notebook. These correspond to the task requirements and are categorized by markdown headers indicating which task they are a part of.

## AI Usage Note
`(1) tools used (2) key prompts (3) how results were changed and verified`

`(1)` I used Copilot with GPT Codex for a handful of data visualization.

- `(2)` "Can you create a regex pattern to remove all special characters from a string"
    - `(3)` I verified correctness by going to an online regex sandbox, plugging in the pattern, and testing that it detected the characters I expected. Then I put it into a function to manipulate dataframes, checked the output, and verified that the values I expected had been correctly aggregated.
- `(2)` "Please explain how `loc` works to me, and how it is different from `iloc`.
- `(2)` "Please refresh me on how `groupby` works, and how I turn the GroupBy object back into a dataframe"
- `(2)` "Please plot the NUMBER_OF_SEATS_BOXCOX columns and the CAPACITY_IN_POUNDS_BOXCOX columns from the df_task4 and df_task4_dropzero dataframes I just created. Arrange the 4 histograms in a grid so I can view them side by side."
    - `(3)` I had already previously plotted the distributions myself, but wanted to arrange them into subplots for easier comparison without doing the menial labor of setting indices and relabeling everything. Because I had already plotted individual histograms myself, I could verify that the results looked correct.
- `(2)` "Please refactor this code that creates the SIZE column into a function that takes a dataframe and returns the modified version"
    - `(3)` Again, as I had already implemented the functionality but forgot to put it into a function, I could verify the correctness while letting AI save me time.