import pandas as pd
import upurch as pc

pt = pc.PurchTools()
docs = pt.loadxmltoora()
#print(docs)
writer = pd.ExcelWriter('dfdoc.xlsx')
docs.to_excel(writer)
writer.save()
