<h1>French zip codes and departments analysis with Spark</h1>

<h2>Datasets :</h2>
- Zip Codes - laposte_hesasmal.csv -  https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/ 
- French departments - departement.csv - https://sql.sh/1879-base-donnees-departements-francais

<h2> Zip codes</h2>
The obtained data frame is column-oriented distributed
Contains 6 columns of float, string, and int types

I extracted the following information :
- File layout
- The number of municipalities
- The number of municipalities with the Ligne_5 attribute
- The department which has the most communes
- The municipalities in the Aisne department

And added a column to the data containing the commune's department number. 


The result is available in the CSV file "commune_et_departement.csv", with columns Code_commune_INSEE, Nom_commune, Code_postal, and department, ordered by postal code.

