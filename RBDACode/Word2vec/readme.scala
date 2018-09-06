//1. prepare: run spark-shell on dumbo
//module load spark/2.2.0(or could do spark/2.3.0)
//spark-shell --driver-memory 40g (this is to require more memory)

//2. prepare data for Word2vec Model:
val textFile = spark.read.textFile("project/Jason/Jason.txt") // put the hdfs path for the original file here
for(year <- 1990 to 2018 ){
		val words_year = textFile.filter(line => line.contains(year.toString))
        val records = words_year.map(line => line.replaceAll("[^a-zA-Z ]", "").toLowerCase().trim().split("\t")(0))
		records.write.format("text").save("project/Jason/"+year.toString)
    }

//This loop extract the contents of dataset of arXiv
// format of document: content(String) \t  year
//split("\t"): the content and year is seperated by "\t"

//3. import package and run model
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
//Could also refer to https://spark.apache.org/docs/2.2.0/mllib-feature-extraction.html
//there is an example on how to train the model based on one document
//https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.mllib.feature.Word2Vec (parameters guide)

for(year <- 2013 to 2018 ){
		val path = "project/Jason/" + year.toString + "/*" // read all files under the directory "project/Jason/year"
		// since in the step above, we constructed such directory
		val savepath = "project/Jason/model1/" + year.toString
		val input = sc.textFile(path).map(line => line.trim().split(" ").toSeq)
		val word2vec = new Word2Vec()
		//model parameters: control the time to run the model. 

		//select word through its count in whole training dataset. 
		//larger than this number the word will present in final dictionary
		word2vec.setMinCount(100) 

		//how the model deal with sentence. recommend change according to input
		//the dimension of final result (each word is represented by a vector with 1000 dimension)
		word2vec.setMaxSentenceLength(300) 

		//can also change into a smaller number. Speed up the training
		word2vec.setVectorSize(1000) 
		
		//to decide how much words to take when calculating loss function
		word2vec.setWindowSize(10)

		//trigger the model
		val model = word2vec.fit(input)

		//save model
		model.save(sc, savepath)
    }

    //load model
    val sameModel = Word2VecModel.load(sc, "myModelPath")
    //find n Synonyms, the closest n words related to the target word
    //target is the string of the target word, n is the number(int) of the neighbor of the target word we want to get
    val synonyms = model.findSynonyms(target, n)
    //if we don't want to plot the high dimensional data, now we are done~


