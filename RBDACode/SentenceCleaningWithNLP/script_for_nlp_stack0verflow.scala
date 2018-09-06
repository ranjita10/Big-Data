import com.johnsnowlabs.nlp.pretrained.pipelines.en.BasicPipeline
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators._

val lines = sc.textFile("RBDASpark/Ranjita.txt")
val records = lines.map(_.split("\t"))
val records1 = records.filter(rec => rec(0).trim().length != 0)
val records = records1

case class wordListClass(text: String, year: String)
val text = records.map{case Array(s0, s1) => wordListClass(s0, s1)}
val text_df = text.toDF("text", "year")

val finisher = new Finisher().setInputCols("normal")
val basicPipeline = BasicPipeline().pretrained()

val pipeline = new Pipeline().setStages(Array(basicPipeline, finisher))

val ann = pipeline.fit(spark.emptyDataFrame).transform(text_df).select("finished_normal", "year")

val fn = ann.withColumn("finished_normal", concat_ws(" ",col("finished_normal"))).withColumnRenamed("finished_normal", "text")

val finisher2 = new Finisher().setInputCols("lemma")
val pipeline2 = new Pipeline().setStages(Array(basicPipeline, finisher2))

pipeline2.fit(spark.emptyDataFrame).transform(fn).show()

val result = pipeline2.fit(spark.emptyDataFrame).transform(fn).select("finished_lemma", "year")
val final_result = result.withColumn("finished_lemma", concat_ws(" ",col("finished_lemma"))).withColumnRenamed("finished_lemma", "text")

import org.apache.spark.ml.feature.StopWordsRemover

val remover = new StopWordsRemover().setInputCol("finished_lemma").setOutputCol("filtered")

val result_without_stopwords = remover.transform(result)

val output = result_without_stopwords.withColumn("filtered", concat_ws(" ",col("filtered"))).withColumnRenamed("filtered", "text").select("text", "year")

val output1 = output.select(concat($"text", lit("\t"), $"year"))

output1.write.text("RBDACleanData/RanjitaNLP")