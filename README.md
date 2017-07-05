# memexDomainRank
Tools to parse an HTML domain discovery collection and rank pages of interest

This effort is split into 3 major milestones:
  * Parse links from HTML
  * Collect/count links
  * Compute metrics
  
  **Parse Links**
  The parsing portion include three different parsing styles.  They are: Complete hyperlink (drop parameters, anchors); everything up to the first slash (e.g. https://london.craigslist.co.uk/); only the domain name and TLD (craigslist.co.uk).  The example shows a TLD that contains two ".", for example .co.uk.  Typical TLD's only contain one "." such as: .com, .net, etc.
  
 **Collect and Count** (coming soon)
 The collect and count portion imports the links parsed previously and aggragates them.
 
 **Compute Metrics** (coming soon)
 Sites of interest can be identified based on the ratio of in-links to out-links, pagerank, etc.  Metrics are computed and exported for each site.  Finally, the sites can be sorted by their metrics to yield a ranked list of sites which are then appropriate for human vetting.
 
