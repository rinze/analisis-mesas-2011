import csv
import zipfile
import pandas as pd

def get_parties(parties_file_handler):
    """
    Return the parties along with their numerical code as a
    hashtable (key => code, value => party)
    """

    res = dict()
    for line in parties_file_handler:
        # Assign fields of fixed length according to the documentation.
        code = line[8:14].decode("ascii") # numeric
        party = line[64:214].strip().decode("iso-8859-1")
        res[code] = party

    return(res)

def get_election_results(election_file_handler, parties_dict, 
                         filter_prov_code = '28', 
                         filter_town_code = '079'):
    """
    Return elections results at the level of individual urn. Returns a Pandas 
    DataFrame with the relevant fields.

    This function doesn't use the information from INE which translates province 
    and town codes to actual names. We are just interested in Madrid.

    See http://www.ine.es/daco/daco42/codmun/codmun11/11codmunmapa.htm for other codes.
    """
    def parse_line(line):
        # As in getParties(), parse according to official field description.
        prov_code = line[11:13].decode("ascii")
        town_code = line[13:16].decode("ascii")

        if (not filter_prov_code and not filter_town_code) or \
            (prov_code == filter_prov_code and town_code == filter_town_code):
            dist_code = line[16:18].decode("ascii")
            section_code = line[18:21].decode("ascii")
            table_code = line[22:23].decode("ascii")
            party_code = line[23:29].decode("ascii")
            votes = int(line[29:36].decode("ascii"))
            
            entry = dict(prov_code = prov_code, town_code = town_code, 
                         dist_code = dist_code, section_code = section_code, 
                         table_code = table_code, party_code = party_code, 
                         party_name = parties_dict[party_code], 
                         votes = votes)
            return(entry)

    res = [parse_line(x) for x in election_file_handler]
    res = [x for x in res if x]

    return(pd.DataFrame(res))


def process_election(zip_file_path, filter_prov_code = '28', filter_town_code = '079'):
    """
    Processes the full election data contained in `zip_file_path`.
    """
    with zipfile.ZipFile(zip_file_path, "r") as zfile:
        listfiles = zfile.namelist()
        for lf in listfiles:
            # Do it this way because some times the .zip file contains nested folders.
            basename = lf.split("/")[-1]
            # Parties file
            if basename.startswith("03"):
                with zfile.open(lf, "r") as fhandler:
                    parties_dict = get_parties(fhandler)
            # Results file        
            if basename.startswith("10"):
                with zfile.open(lf, "r") as fhandler:
                    res = get_election_results(fhandler, parties_dict, 
                                               filter_prov_code, filter_town_code)
                    
    return res

def find_suspicious(eldata):
    """Finds suspicious results in the provided df (already filtered by province 
    / town).
    """

    # Total at the ballot box level
    eldata_total = eldata.groupby(['section_code', 'table_code', 'dist_code'])\
	.agg({'votes': 'sum'})\
	.rename(columns = {'votes': 'total_votes'})\
	.reset_index()
    n = eldata_total.shape[0]
    eldata = pd.merge(eldata, eldata_total)
    eldata['pct_votes'] = eldata['votes'] / eldata['total_votes']

    # Average at the party level
    eldata_stats = eldata.groupby('party_name')\
	.agg({'pct_votes': 'mean'})\
	.rename(columns = {'pct_votes': 'mean'})\
	.reset_index()
    eldata_stats['threshold'] = 20 * eldata_stats['mean']
    eldata_with_stats = pd.merge(eldata, eldata_stats)

    bad_data = eldata_with_stats.loc[(eldata_with_stats['pct_votes'] > eldata_with_stats['threshold']) & (eldata_with_stats['votes'] > 20), :]\
	.groupby(['dist_code', 'section_code', 'table_code'])\
	.agg({'pct_votes': 'count'})\
	.rename(columns = {'pct_votes': 'bad_counts'})\
	.reset_index()\
	.sort_values('bad_counts', ascending = False)

    # Total ballot boxes
    return bad_data, n

def find_suspicious_2(eldata):
    """Better method than the previous one.

    For this one, we return ballot boxes in which a party that had less than 2 % 
    of the vote, on average, has more than 5 %. This should be enough reason to 
    inspect these results.
    """

    # Total at the ballot box level
    eldata_total = eldata.groupby(['section_code', 'table_code', 'dist_code'])\
	.agg({'votes': 'sum'})\
	.rename(columns = {'votes': 'total_votes'})\
	.reset_index()
    n = eldata_total.shape[0]
    eldata = pd.merge(eldata, eldata_total)
    eldata['pct_votes'] = eldata['votes'] / eldata['total_votes']

    # Average at the party level
    party_average = eldata.groupby('party_name')\
	.agg({'pct_votes': 'mean'})\
	.rename(columns = {'pct_votes': 'mean'})\
	.reset_index()

    party_average = party_average.loc[party_average['mean'] < 0.02, :]
    print(party_average)

    bad_data = pd.merge(eldata, party_average, how = "inner")\
        .loc[(eldata['pct_votes'] > 0.05), :]\
	.groupby(['dist_code', 'section_code', 'table_code'])\
	.agg({'pct_votes': 'count'})\
	.rename(columns = {'pct_votes': 'bad_counts'})\
	.reset_index()\
	.sort_values('bad_counts', ascending = False)

    print(bad_data.head())

    return bad_data, n
