library(tidyverse)
library(topicmodels) #the LDA algorithm
library(tidytext)

read_docx <- textreadr::read_docx

transcript_dir <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Qualitative/In-Depth EMA Map Follow-Up Interview Qual Data/Transcripts"



transcript_files <- read_file_list(transcript_dir, file_filter = ".docx$", hour_filter = FALSE, recursive = FALSE)
interviewers <- c("sara", "brian", "lindsee", "andrew","vi","justine","lopez")

drop_person <- function(wordlist, personlist) {
    for(x in personlist){
      wordlist <- gsub(paste0(x,":.*"),"",wordlist)
    }
  return(wordlist)
}

core_count <- detectCores() - 1L
registerDoParallel(cores = core_count)


full_transcripts <- foreach(file_to_read = transcript_files, .combine = rbind, .packages = c("tidyverse")) %dopar% {
  interview <- tolower(read_docx(file_to_read))
  filenameonly <- str_replace(file_to_read,transcript_dir,"")
  intname <- substr(filenameonly,5,8)
  clear_interviewer <- drop_person(interview,interviewers)
  dropped_interviewee <- gsub(paste0(intname,":"),"",clear_interviewer)
  drop_blanks <- dropped_interviewee[dropped_interviewee != ""]
  
  ready_to_tokenize <- as_tibble(drop_blanks[1:length(drop_blanks)-1]) %>%
    unnest_tokens(word,value) %>%
    mutate(word = str_replace(word,"’","'")) %>%
    mutate(word = str_replace(word,"glbt","lgbt")) %>%
    anti_join(stop_words) %>%
    mutate(nonmissnum = as.numeric(word)) %>%
    filter(is.na(nonmissnum)) %>%
    select(-nonmissnum) %>%
    count(word, sort = TRUE) %>%
    mutate(document = intname)
  
  ready_to_tokenize
}

maxword <- full_transcripts %>%
  group_by(word) %>%
  summarise(counter = sum(n)) %>%
  arrange(-counter) %>%
  filter(counter > 100)

wordlist <- c("yeah","inaudible","gonna","lot","mm","pause",
              "hmm","affirmative","bit","wanna","um",
              "speaker","shit","vi","crosstalk","hey",
              "literally","kinda","stop","uh","la","freakin",
             "chuckles","person","mhmm","fucking","laughs","hm",
             "eh","everything","everything's","yea",
              "people","people's","pretty","guess","makes","sense","time","times","gotta",
              "gonna","fuck","im","bitch","huh","yup","yo","cuz", "feel", "stuff","hell",
             "whatnot","happened","supposed","day")

for(x in wordlist){
  full_transcripts <- filter(full_transcripts,word != x)
}


new_maxword <- full_transcripts %>%
  group_by(word) %>%
  summarise(counter = sum(n)) %>%
  arrange(-counter)

tester <- cast_dtm(full_transcripts, document = document, term = word, value = n)
burnin = 1000
iter = 1000
keep = 50
k = 30 # or 10
set.seed(1234)

testerlda <- LDA(tester, k = k, method = "Gibbs",
                 control = list(burnin = burnin, iter = iter, keep = keep))
chapters_lda_td <- tidy(testerlda)
person_terms1 <- tidy(testerlda, matrix = "gamma") %>%
  arrange(-gamma)
person_terms <-  as.data.frame(testerlda@gamma)
topic_terms <- as.matrix(terms(testerlda,50))

topic = 1
newdf <- data.frame(term = testerlda@terms, p = exp(testerlda@beta[topic,]))
head(newdf[order(newdf$p),])

wordcloud(words = newdf$term,
          freq = newdf$p,
          max.words = 200,
          random.order = FALSE,
          rot.per = 0.35,
          colors=brewer.pal(8, "Dark2"))

top_terms <- chapters_lda_td %>%
  group_by(topic) %>%
  top_n(5, beta) %>%
  ungroup() %>%
  arrange(topic, -beta)

top_terms %>%
  mutate(term = reorder(term, beta)) %>%
  ggplot(aes(term, beta, fill = factor(topic))) +
  geom_bar(alpha = 0.8, stat = "identity", show.legend = FALSE) +
  facet_wrap(~ topic, scales = "free", ncol = 3) +
  coord_flip()

topic1ToTopic2 <- lapply(1:nrow(tester),function(x)
  sort(person_terms[x,])[10]/sort(person_terms[x,])[10-1])



full_data  <- cast_dtm(full_transcripts, document = document, term = word, value = n)
n <- nrow(full_data)
#-----------validation--------
k <- 5

splitter <- sample(1:n, round(n * 0.75))
train_set <- full_data[splitter, ]
valid_set <- full_data[-splitter, ]

fitted <- LDA(train_set, k = k, method = "Gibbs",
              control = list(burnin = burnin, iter = iter, keep = keep) )
perplexity(fitted, newdata = train_set) # about 1460
perplexity(fitted, newdata = valid_set) # about 2400


#----------------5-fold cross-validation, different numbers of topics----------------
# set up a cluster for parallel processing
cluster <- makeCluster(detectCores(logical = TRUE) - 1) # leave one CPU spare...
registerDoParallel(cluster)

# load up the needed R package on all the parallel sessions
clusterEvalQ(cluster, {
  library(topicmodels)
})

folds <- 5
splitfolds <- sample(1:folds, n, replace = TRUE)
candidate_k <- c(10, 20, 30, 40, 50) # candidates for how many topics

# export all the needed R objects to the parallel sessions
clusterExport(cluster, c("full_data", "burnin", "iter", "keep", "splitfolds", "folds", "candidate_k"))

# we parallelize by the different number of topics.  A processor is allocated a value
# of k, and does the cross-validation serially.  This is because it is assumed there
# are more candidate values of k than there are cross-validation folds, hence it
# will be more efficient to parallelise
system.time({
  results <- foreach(j = 1:length(candidate_k), .combine = rbind) %dopar%{
    k <- candidate_k[j]
    results_1k <- matrix(0, nrow = folds, ncol = 2)
    colnames(results_1k) <- c("k", "perplexity")
    for(i in 1:folds){
      train_set <- full_data[splitfolds != i , ]
      valid_set <- full_data[splitfolds == i, ]
      
      fitted <- LDA(train_set, k = k, method = "Gibbs",
                    control = list(burnin = burnin, iter = iter, keep = keep) )
      results_1k[i,] <- c(k, perplexity(fitted, newdata = valid_set))
    }
    return(results_1k)
  }
})
stopCluster(cluster)

results_df <- as.data.frame(results)

ggplot(results_df, aes(x = k, y = perplexity)) +
  geom_point() +
  geom_smooth(se = FALSE) +
  ggtitle("5-fold cross-validation of topic modelling with the 'LML' dataset",
          "(ie five different models fit for each candidate number of topics)") +
  labs(x = "Candidate number of topics", y = "Perplexity when fitting the trained model to the hold-out set")

