import atoma, requests, csv, os.path, re


category = ["cs.AI", "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CL", "cs.CR", "cs.CV", "cs.CY", "cs.DB", "cs.DC", "cs.DL", "cs.DM", "cs.DS", "cs.ET", "cs.FL", "cs.GL", "cs.GR", "cs.GT", "cs.HC", "cs.IR", "cs.IT", "cs.LG", "cs.LO", "cs.MA", "cs.MM", "cs.MS", "cs.NA", "cs.NE", "cs.NI", "cs.OH", "cs.OS", "cs.PF", "cs.PL", "cs.RO", "cs.SC", "cs.SD", "cs.SE", "cs.SI", "cs.SY"]
file_name = ["csAI.csv", "csAR.csv", "csCC.csv", "csCE.csv", "csCG.csv", "csCL.csv", "csCR.csv", "csCV.csv", "csCY.csv", "csDB.csv", "csDC.csv", "csDL.csv", "csDM.csv", "csDS.csv", "csET.csv", "csFL.csv", "csGL.csv", "csGR.csv", "csGT.csv", "csHC.csv", "csIR.csv", "csIT.csv", "csLG.csv", "csLO.csv", "csMA.csv", "csMM.csv", "csMS.csv", "csNA.csv", "csNE.csv", "csNI.csv", "csOH.csv", "csOS.csv", "csPF.csv", "csPL.csv", "csRO.csv", "csSC.csv", "csSD.csv", "csSE.csv", "csSI.csv", "csSY.csv"]


for i in range(0, 5):
    for index in range(0, 40):

        if not os.path.exists(file_name[index]):
            with open(file_name[index], 'w') as cfile:
                create_file = csv.writer(cfile)

        with open(file_name[index], 'r') as rfile:
            with open(file_name[index], 'a') as wfile:
                start = -1
                lines = csv.reader(rfile)
                for line in lines:
                    start = int(line[1])

                lines_writer = csv.writer(wfile)

                start +=1

                running = True
                while running:
                    url = 'http://export.arxiv.org/api/query?search_query=cat:' + category[index] + '&start=' + str(start) + '&max_results=1000'
                    print (url)
                    response = requests.get(url)
                    feed = atoma.parse_atom_bytes(response.content)

                    for entry in feed.entries:
                        print(index, category[index], start)

                        title = re.sub(r"[,]+", ' ', entry.title.value)
                        title = re.sub(r"\s+", ' ', title).strip()

                        summary = re.sub(r"[,]+", ' ', entry.summary.value)
                        summary = re.sub(r"\s+", ' ', summary).strip()

                        lines_writer.writerow([category[index], start, title, summary, entry.published.year])

                        start += 1

                    if len(feed.entries) < 1000:
                        running = False