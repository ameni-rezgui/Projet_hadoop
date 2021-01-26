/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projetevaluation;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author hadoop
 */
public class ModeleMapReduceMapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
     // allocation mémoire de la clé et valeur de sortie
    private Text cleIT = new Text();
    private DoubleWritable valeurIT = new DoubleWritable();
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
            cleIT.set(new Text(""+sale.getItemType()+""));
            valeurIT.set(sale.getTotalProfit());
            // émettre la paire (clé, valeur) vers le reducer
            context.write(cleIT, valeurIT);

        } catch (Exception e) {
            // ignorer l'exception ou l'afficher => consulter les logs de stderr
            //System.err.println(e.getStackTrace()[0]+": "+e.getMessage()+" => "+e+" sur la ligne "+ligne);
        }
    }
}
