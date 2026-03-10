# ANALYSE-DES-PERFORMANCES-FOURNISSEURS

🎯 But du projet :

Développer un tableau de bord complet pour analyser la performance des fournisseurs d’une entreprise de transport. L’objectif est de centraliser, structurer et visualiser les données afin de faciliter le suivi des indicateurs clés et d’améliorer la prise de décision stratégique.

🔍 Objectifs du projet :

• Suivre la qualité des prestations fournies (délais, conformité, incidents).
• Identifier les fournisseurs à faible performance ou à risque.
• Comparer les performances entre différents prestataires.
• Proposer des recommandations d’optimisation pour réduire les coûts et améliorer la fiabilité des partenaires.

🛠 Outils & Technologies :

• Power BI
• Power Query
• SQL
• Excel
• Dataset Kaggle “Workplace Safety” (détourné pour simuler des données fournisseurs)

Résultats : 

L’analyse exploratoire des données a permis de mieux comprendre la structure et la qualité du jeu de données relatif aux performances des vendeurs. En combinant plusieurs tables issues de la base de données SQLite (purchases, purchase_prices, vendor_invoice et sales), un jeu de données consolidé a été construit afin d’évaluer la performance globale des fournisseurs. 

Le processus de nettoyage des données a permis d’identifier et de corriger certaines incohérences telles que les valeurs manquantes, les profits négatifs et les valeurs aberrantes. À travers l’utilisation de statistiques descriptives et de visualisations (histogrammes, boxplots et graphiques de distribution), l’analyse a mis en évidence les vendeurs générant les plus fortes contributions en termes de ventes et de revenus, tout en révélant certaines transactions présentant des marges négatives.

L’étude a également permis d’analyser la distribution des prix d’achat, des quantités vendues et des revenus générés. Enfin, un tableau synthétique des performances des vendeurs a été généré et stocké dans la base de données afin de faciliter les analyses futures et la création éventuelle de tableaux de bord décisionnels.
