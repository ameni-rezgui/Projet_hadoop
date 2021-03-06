/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projetevaluation;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author hadoop
 */
public class ModeleMapReduceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>  {
    // allocation mémoire de la clé et valeur de sortie
    private Text cleI = new Text();
    private DoubleWritable valeurI = new DoubleWritable();
    
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {
        // TODO si besoin, ignorer les premières lignes contenant les titres
        //if (cleE.get() <= ???) return;

        // données d'entrée provenant des fichiers à traiter
        String ligne = valeurE.toString();
        // aucune paire (clé, valeur) ne sera produite en cas d'exception
        try {

            // TODO séparer la ligne en champs

            // TODO définir la clé de sortie
           Sales sale = new Sales(ligne);
            cleI.set(new Text(""+sale.getRegion()+""));
            valeurI.set(sale.getTotalProfit());
            // émettre la paire (clé, valeur) vers le reducer
            context.write(cleI, valeurI);

        } catch (Exception e) {
            // ignorer l'exception ou l'afficher => consulter les logs de stderr
            //System.err.println(e.getStackTrace()[0]+": "+e.getMessage()+" => "+e+" sur la ligne "+ligne);
        }
    }
   
}
   
