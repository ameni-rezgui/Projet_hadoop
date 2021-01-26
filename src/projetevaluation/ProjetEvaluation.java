/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projetevaluation;


import java.io.InputStreamReader;


import java.io.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.*;

/**
 *
 * @author hadoop
 */
public class ProjetEvaluation {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        // nom complet du fichier
    Path nomcomplet = new Path("/home/hadoop/NetBeansProjects/ProjetEvaluation/sales_world_10k.csv");
    // ouvrir le fichier sur HDFS
        Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream inStream = fs.open(nomcomplet);
    try {
    // préparer un lecteur séquentiel
    InputStreamReader isr = new InputStreamReader(inStream);
    BufferedReader br = new BufferedReader(isr);
    // parcourir les lignes une par une
    String ligne = br.readLine();
    while (ligne != null) {
    // traiter la ligne courante

    System.out.println(ligne);
    // passer à la ligne suivante (retourne null si fin fichier)
    ligne = br.readLine();
    }
    } finally {
    // fermer le fichier quoi qu'il arrive
    inStream.close();
    fs.close();
    }
    }
 
    
}
