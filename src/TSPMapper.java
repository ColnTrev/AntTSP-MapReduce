import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by colntrev on 4/8/18.
 * This mapper implements the logic from the open source code available from the authors of
 *  Ant System: Optimization by a Colony of Cooperating Agents
 *  This code is for use in testing a MASS adaptation of the algorithm described in the above paper
 *
 *  MapReduce implementation was inspired by a similar approach to solving kmeans clustering using
 *  a batch based approach. Each mapper executes Ant TSP and a reducer task finds the best tour from all mappers.
 */
public class TSPMapper extends Mapper<NullWritable, IntWritable, IntWritable, Text> {
    private double[][] Graph = null;
    private double[][] trails = null;
    private Ant[] workers = null;
    private double[] probs = null;
    private int[] bestTour = null;

    private Random rand = new Random();
    private double c = 1.0;
    private double alpha = 1.0;
    private double beta = 5.0;
    private double evaporation = 0.5;
    private double Q = 500;
    private double pr = 0.01;
    private double tourLength;
    private double numAntFactor = 0.8;
    private int currentIndex = 0;
    private int towns;
    private int ants;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path graphPath = new Path(context.getConfiguration().get("paths.graph"));
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(graphPath)));
        String line;
        int i = 0;

        while ((line = br.readLine()) != null) {
            String[] splitG = line.split(" ");
            List<String> tokens = new ArrayList<>();
            for(String tok : splitG){
                if(!tok.isEmpty()){
                    tokens.add(tok);
                }
            }
            if (Graph == null) {
                Graph = new double[tokens.size()][tokens.size()];
            }

            int j = 0;
            for (String s : tokens) {
                if (!s.isEmpty()) {
                    Graph[i][j++] = Double.parseDouble(s) + 1;
                }
            }
            i++;
        }
        towns = Graph.length;
        ants = (int)(towns * numAntFactor);
        trails = new double[towns][towns];
        probs = new double[towns];
        workers = new Ant[ants];
        initAnts();
    }

    @Override
    protected void map(NullWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        // perform Ant TSP using the Graph data
        // Each Mapper performs its own version of Ant TSP
        // key is ignored
        // value is used to kick off map tasks
        // input file should be structured as follows
        // 1
        // 2
        // 3
        // to indicate the number of map tasks to generate. Value is otherwise ignored
        char delimiter = ' ';
        int iteration = 0;
        int maxIterations = context.getConfiguration().getInt("limit", 2000);
        clearTrails();
        while(iteration < maxIterations){
            setupAnts();
            moveAnts();
            updateTrails();
            updateBest();
            iteration++;
        }

        Integer[] bt = new Integer[bestTour.length];
        for(int i = 0; i < bt.length; i++){
            bt[i] = bestTour[i];
        }

        List<Integer> tour = new ArrayList<>(Arrays.asList(bt));

        StringBuilder sb = new StringBuilder();
        sb.append(tour);
        sb.append(delimiter);
        sb.append(new Double(tourLength).toString());

        context.write(new IntWritable(1), new Text(sb.toString()));
    }

    private class Ant {
        public int tour[] = new int[Graph.length];
        public boolean visited[] = new boolean[Graph.length];

        public void visitTown(int town) {
            tour[currentIndex + 1] = town;
            visited[town] = true;
        }

        public boolean visited(int i) {
            return visited[i];
        }

        public double tourLength() {
            double length = Graph[tour[towns - 1]][tour[0]];
            for (int i = 0; i < towns - 1; i++) {
                length += Graph[tour[i]][tour[i + 1]];
            }
            return length;
        }

        public void clear() {
            for (int i = 0; i < towns; i++)
                visited[i] = false;
        }
    }

    protected void initAnts(){
        for(Ant a : workers){
            a = new Ant();
        }
    }

    protected double pow(final double a, final double b) {
        final int x = (int) (Double.doubleToLongBits(a) >> 32);
        final int y = (int) (b * (x - 1072632447) + 1072632447);
        return Double.longBitsToDouble(((long) y) << 32);
    }

    private int selectNextTown(Ant ant){
        int j = -1;
        if(rand.nextDouble() < pr){
            int t = rand.nextInt(towns - currentIndex);
            for(int i = 0; i < towns; i++){
                if(!ant.visited(i)){
                    j++;
                }
                if(j == t){
                    return i;
                }
            }
        }
        probTo(ant);
        double r = rand.nextDouble();
        double total = 0.0;
        for(int i = 0; i < towns; i++){
            total += probs[i];
            if(total >= r){
                return i;
            }
        }
        return j; // should never get this
    }
    private void probTo(Ant ant){
        int i = ant.tour[currentIndex];
        double denom = 0.0;
        for(int l = 0; l < towns; l++){
            if(!ant.visited(l)){
                denom += pow(trails[i][l], alpha) * pow(1.0 / Graph[i][l], beta);
            }
        }

        for(int j = 0; j < towns; j++){
            if(ant.visited(j)){
                probs[j] = 0.0;
            } else {
                double numerator = pow(trails[i][j], alpha) * pow(1.0 / Graph[i][j], beta);
                probs[j] = numerator / denom;
            }
        }
    }

    private void moveAnts() {
        while(currentIndex < towns - 1){
            for(Ant a : workers){
                a.visitTown(selectNextTown(a));
            }
            currentIndex++;
        }
    }

    private void setupAnts(){
        currentIndex = -1;
        for(Ant a : workers){
            a.clear();
            a.visitTown(rand.nextInt(towns));
        }
        currentIndex++;
    }
    private void updateTrails(){
        calculateEvaporation();
        for(Ant a : workers){
            double contribution = Q / a.tourLength();
            for(int i = 0; i < towns; i++){
                trails[a.tour[i]][a.tour[i+1]] += contribution;
            }
            trails[a.tour[towns - 1]][a.tour[0]] += contribution;
        }
    }

    private void clearTrails(){
        for(int i = 0; i < towns; i++){
            for(int j = 0; j < towns; j++){
                trails[i][j] = c;
            }
        }
    }
    private void calculateEvaporation(){
        for(int i = 0; i < towns; i++){
            for(int j = 0; j < towns; j++){
                trails[i][j] *= evaporation;
            }
        }
    }
    private void updateBest(){
        if(bestTour == null){
            bestTour = workers[0].tour.clone();
            tourLength = workers[0].tourLength();
        }
        for(Ant a : workers){
            if(a.tourLength() < tourLength){
                tourLength = a.tourLength();
                bestTour = a.tour.clone();
            }
        }
    }
}
