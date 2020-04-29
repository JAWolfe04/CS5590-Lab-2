package lab2.part4;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.desc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

public class App 
{
    public static void main( String[] args )
    {
    	// Get Spark Session
    	SparkSession spark = SparkSession.builder().appName("graphing").master("local[*]").getOrCreate();
    	
    	// Make Schemas to implement in the creation of dataframes
    	StructType edgeSchema = new StructType(new StructField[] {
    			new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("group1", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("group2", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("weight", DataTypes.IntegerType, false, Metadata.empty())
    	});
    	
    	StructType vertexSchema = new StructType(new StructField[] {
    			new StructField("group_id", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("group_name", DataTypes.StringType, false, Metadata.empty()),
    			new StructField("num_members", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("category_id", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("category_name", DataTypes.StringType, false, Metadata.empty()),
    			new StructField("organizer_id", DataTypes.IntegerType, false, Metadata.empty()),
    			new StructField("group_urlname", DataTypes.StringType, false, Metadata.empty())
    	});
    	
    	// Pull edges and vertices from csv files with supplied schema
    	Dataset<Row> edgeDF = spark.read().option("header", "true").schema(edgeSchema)
    			.csv("C:\\Users\\Jonathan\\Desktop\\UMKC\\CS 5590\\Labs\\CS5590-Lab-2\\Part_4\\Input\\group-edges.csv");
    	Dataset<Row> vertexDF = spark.read().option("header", "true").schema(vertexSchema)
    			.csv("C:\\Users\\Jonathan\\Desktop\\UMKC\\CS 5590\\Labs\\CS5590-Lab-2\\Part_4\\Input\\meta-groups.csv");
    	
    	// We only need to know the connections and a few attributes of the groups for interpretation
    	edgeDF = edgeDF.select("group1", "group2");
    	vertexDF = vertexDF.select("group_id", "group_name", "num_members");
    	
    	// Need to supply the names as expected by GraphX
    	edgeDF = edgeDF.withColumnRenamed("group1", "src");
    	edgeDF = edgeDF.withColumnRenamed("group2", "dst");
    	vertexDF = vertexDF.withColumnRenamed("group_id", "id");
    	
    	// Need to add a 2 way connection between groups with shared members
    	Dataset<Row> switchedEdges = edgeDF.withColumnRenamed("src", "dst1");
    	switchedEdges = switchedEdges.withColumnRenamed("dst", "src");
    	switchedEdges = switchedEdges.withColumnRenamed("dst1", "dst");
    	edgeDF = edgeDF.union(switchedEdges);
    	
    	// Remove duplicate rows
    	vertexDF = vertexDF.distinct();
    	edgeDF = edgeDF.distinct();
    	
    	GraphFrame graph = new GraphFrame(vertexDF, edgeDF);
    	
    	// Using more than 3 max iterations takes a long time to run
    	GraphFrame result = graph.pageRank().resetProbability(0.15).maxIter(3).run();
    	
    	// Sort by pagerank to easily see the most influential groups to the least
    	result.vertices().sort(desc("pagerank")).coalesce(1).write().option("header", "true")
		.csv("C:\\Users\\Jonathan\\Desktop\\UMKC\\CS 5590\\Labs\\CS5590-Lab-2\\Part_4\\Output\\PageRank.csv");
    }
}
