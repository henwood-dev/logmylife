### Introduction to R 
## Using Real-Time Alcohol Sensor Data
#
## Eldin Dzubur
###


# Important Commands:
# library(): loads packages into the current R environment for use
library(tidyverse) # Tidyverse is a package compilation known for its comprehensive documentation and useful tools.
library(readr) # Part of tidyverse, readr allows us to read/write non-R files, such as CSV and Stata
library(haven) # special reader program
library(stringr) # powerful string manipulation component of tidy
library(lubridate) # powerful datetime package from tidy
library(ggplot2) # Gorgeous graphical language used everyhwere online

# For our alcohol data, much of it is saved as SPSS and CSV files
# We want to avoid using Excel files, so any Excel files will be ignored.
# Let's read data using <- and read_sav(). <- is the same as = in R, but is more conventional when assigning objects.
baseline_data <- read_sav("C:/Users/dzubur/Desktop/TAC/2017 Crawl/Baseline data/Senior Bar Crawl_Baseline_2017.sav")
attributes(baseline_data) # attributes gives us info about an object
# The tidy paradigm saves data as "tibbles" which are highly parameterized extensions of R's data.frames
# They are slightly faster and significantly more human-readable, but faster options exist for power users.

# We can extract vectors of data using column identifiers
ids <- baseline_data$Identifier
# However, we don't have to always assign an object when calling functions
unique(baseline_data$Q1_Gender) # Here, we see that 1 and 2 are the only two gender values, but what do they mean?
# Fortunately, the data is labelled so we can do one of two things:
# 1) Call the column and view the label attribute
attributes(baseline_data$Q1_Gender) # This tells us that the object is of the class labelled in SPSS format
# 2) Create (or "mutate") a new variable called gender that is a factor with an undrlying number using as_factor()
baseline_data <- mutate(baseline_data, gender = as_factor(Q1_Gender))
# Notice here that as_factor communicates with the labelled class and automatically knows what to assign.
# Also, note that in the mutate command, we do not have to use $ once we specify a data frame.
# However, as an aside, this is NOT how tidy is written. Instead, we use piping, as follows:
# Piping refers to the data frame being entered as the first argument in the next successive command by
# the %>% symbol.
baseline_data <- baseline_data %>%
  mutate(gender = as_factor(Q1_Gender))
# Here, we are saying Take the data "baseline_data" and insert it as the first argument in the mutate command.
# The mutate command looks for the following spec: mutate(data_name, some variable transformations) so we are
# telling it that data_name = baseline_data using %>%. Then, we specify the mutate command with the remaining
# transformation. Note that just like before, you no longer have to refer to column names with $. The . symbol
# is a data pass parameter that is used by the %>% pipe by default to indicate the previous dataframe. For example,
# the following command is identical to our previous command to improve understanding:
baseline_data <- baseline_data %>%
  mutate(., gender = as_factor(Q1_Gender))
  
# You will see the advantage of the piping approach shortly.
# Let's plot gender quickly:
# To understand how ggplot works, you must understand the concept of aesthetics
# First, ggplot requires specification of a dataset with an x and y aesthetic indicating
# the x and y variables you will plot graphs on. For a histogram, you only need ot plot the x aesthetic:

gender_plot <- baseline_data %>% 
  ggplot(aes(x = gender)) 
# Note that if you plot this graph, you just get male and female on the x axis. You must tell
# ggplot what you want to do with the y values. In this case, we want to get the count of a factor, which is
# better done using the geom_bar() graph
gender_plot + geom_bar()
# As expected, if you already know that you will be using geom_bar, you can chain the commands together:
gender_plot <- baseline_data %>% 
  ggplot(aes(x = gender)) +
  geom_bar()

gender_plot # Now no longer plots a blank graph, because geom_bar was specified

# Now let's take what we need out of baseline data and dump the rest, because we don't need 100 variables of information, yet
# We can use the command "transmute" instead of "mutate" to automatically delete all but the generated variables
# Alternatively, you can play it safe and use "mutate" followed by "select" to select variables you want. 
# However, this would be tedious because we have so many and redundant because we are already mutating them
# Starting again with gender, we also want id
demographic_data <- baseline_data %>%
  transmute(id = Identifier,
            gender = as_factor(Q1_Gender),
            age = Q2_Age,
            weight = Q3_Weight,
            ethnicity = as_factor(Q4_Ethnicity)
            greek = as_factor(Q8_Greek)
            )
