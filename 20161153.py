import os
import sqlparse
from collections import defaultdict
import sys
import operator


arr = ['max','min','Sum','average']
ops = {'>':operator.gt,'<':operator.lt,'>=':operator.ge,'<=':operator.le,'=':operator.eq}
cond = ['<','>','=','<=','>=']

def dbInfo(filename):
    metadata = defaultdict(list)
    try:
        with open(filename,'r') as filereader:
            content = filereader.readlines()
        fr = 0
        content = [x.strip() for x in content]
        length4 = len(content)
        for i in range(length4):
            column = []
            if content[i] == '<begin_table>':
                i = i+1
                tablename = content[i]
                i = i+1
                while content[i] != '<end_table>':
                    column.append(content[i])
                    i=i+1
                metadata[tablename]=column
    except:
        print("Some Error Detected. Please Try Once More")
    return metadata

def quotes_removal(s):
    s = s.strip()
    aq = 1
    while len(s) > aq and (s[aq-1]=='\"' or s[aq-1]=='\'') and s[aq-1]==s[-1]:
        s = s[1:-1]
    return s

def tableInfo(filename,metadata):
    tabledict = []
    wq = 0
    try:
        with open('./files/'+filename+'.csv') as table:
            tdata = table.readlines()
        wq = 0
        tdata = [x.strip() for x in tdata]
        ro = 0
        if len(metadata) == len(tdata[0].split(',')):
            len1 = len(tdata)
            for i in range(len1):
                data = tdata[i].split(',')
                new_data = []
                for d in data:
                    d = quotes_removal(d)
                    s = wq
                    new_data.append(d)
                tabledict.append(new_data)
                ro = ro + 1
                temp = tabledict[i][-1]
                tabledict[i][-1] = temp[:len(temp)]
        else:            print('Update METADATA Dictionary')
    except:        print('Some Error Detwcted.Please Try Once More')
    return tabledict

def attributeInfo(filename):
    attributedata = defaultdict()
    tr = 0
    try:
        with open(filename,'r') as filereader:            content = filereader.readlines()
        content = [x.strip() for x in content]
        con_len = len(content)
        for i in range(con_len):
            if content[i] == '<begin_table>':
                i= i+1
                tr = tr+1
                tablename = content[i]
                i+=1
                while content[i] != '<end_table>':
                    attributedata[content[i]] = tablename
                    tr = tr+1
                    i+=1
    except:
        print("Some Error Detected. Please Try Once More")
    return attributedata

def validate(query):
    ty = 0
    query = query.split()
    if query[ty+1]!='distinct':
        if query[ty] == 'select' and query[ty+2] == 'from':            return True
        else:
            print('INVALID FORMAT Detected. See Correct Usage -> [(select,from,where) are in lowercase')
            sys.exit()
    else:
        if query[ty] == 'select' and query[ty+3] == 'from':            return True
        else:
            print('INVALID FORMAT Detected. See Correct Usage -> [(select,from,where) are in lowercase')
            sys.exit()

def joinTable(table1,table2):
    final = []
    len_table = len(table1)
    if len_table == 0:
        return table1
    tr = 0
    for i in table1:
        for j in table2:
            tr = tr+1
            final.append(i+','+j)
            # print(tr)
    return final

def joinTable1(table1,table2):
    final = []
    len_table = len(table1)
    if len_table == 0:
        return table2
    tr = 0
    for i in table1:
        for j in table2:
            tr = tr+1
            final.append(i+','+j)
            # print(tr)
    return final

def unique(list1):
    unique_list = []
    unique_count = 0
    for x in list1:
        if x not in unique_list:
            unique_count = unique_count+1
            unique_list.append(x)
    print(unique_count)
    return unique_list

def processQuery(query):
    if validate(query):
        query = query.split()
        index4 = 1
        if query[index4] != 'distinct':
            try:
                tablenames = query[index4+2].split(',')
            except:
                print("Invalid Form Of Input,Check Query")
                sys.exit()
            for name in tablenames:
                if name not in tabledata:
                    print('The Table '+name+' does not exists ')
                    sys.exit()
        elif query[1] == 'distinct':
            tablename = query[4]
            if tablename not in tabledata:
                print('The table'+tablename+' does not exists')
                sys.exit()

        limit = 4

        if len(query) == limit:
            if (query[1] == '*'):
                tablename = query[3].split(',')
                dt = 1
                ans = []
                a = []
                for table in tablename:
                    temp = []
                    for row in tabledata[table]:
                        str = ''
                        dt = dt+1
                        for r in row:
                            str += r+','
                        str = str[:-1]
                        temp.append(str)
                    ans.append(temp)
                we = 1
                str2 = ''
                if len(tablename) == we:
                    str = ''
                    for i in metadata[tablename[0]]:
                        str+= tablename[0]+'.'+i+','
                    print(str[:-1])
                    str2 = str2 + str
                    for i in ans:
                        for j in i:
                            print(j)
                else:
                    i = 0
                    a = ans[0]
                    cut = 1
                    while(i<len(ans)-cut):
                        a =joinTable(a,ans[i+1])
                        i = i+1
                    str = ''
                    str2 = ''
                    for table in tablename:
                        cut = 1
                        for i in metadata[table]:
                            str += table+'.'+i+','
                            str2 = str2 + str
                    print(str[:-1])
                    for i in a:
                        print(i)
                sys.exit()

            ty = 0
            by = 1
            qu1 = 3
            if query[1].split('(')[ty] in arr:
                val = query[by].split('(')[by].split(')')[0]
                tablename = query[qu1]
                try:  index = metadata[tablename].index(val)
                except:
                    print("The Given Column Does Not Exist In The Given Table")
                    sys.exit()
                col = []
                entry = 0
                count2 = 0
                average = 0
                sum1 = 0
                min1 = 0
                for data in tabledata[tablename]:
                    entry = entry+1
                    col.append(int(data[index]))

                if query[1].split('(')[0] == 'max':
                    ans = max(col)
                    print('max('+tablename+'.'+val+')')
                    count2 = count2+1
                    # print("max")
                    print(ans)
                    sys.exit()
                
                if query[1].split('(')[0] == 'average':
                    ans = mean(col)
                    average = average + 1
                    # print("average")
                    print('average'+tablename+'.'+col+')')
                    print(ans)
                    count2 = count2+1
                    sys.exit()

                if query[1].split('(')[0] == 'Sum':
                    ans = sum(col)
                    sum1 = sum1+1
                    print('Sum('+tablename+'.'+col+')')
                    # print("Sum")
                    print(ans)
                    count2 = count2+1
                    sys.exit()


                if query[1].split('(')[0] == 'min':
                    ans = min(col)
                    count2 = count2+1
                    print('min('+tablename+'.'+col+')')
                    # print("min")
                    min1 = min1 + 1
                    print(ans)
                    sys.exit()
                
                
            else:
                col_id = 0
                col_list = query[1].split(',')
                que = 3
                tablenames = query[que].split(',')
                tabledict = defaultdict()
                flag = 0
                for col in col_list:
                    flag = 0
                    for table in tablenames:
                        if col in metadata[table]:
                            col_id = col_id+1
                            tabledict[col]=table
                            # print(col)
                            flag = 1
                    if flag == 0:
                        print('Given Columns are not found in given table')
                        sys.exit()


                fu = 1
                count1 = 0
                if len(tablenames)==fu:
                    col_dict = defaultdict()
                    str = ''
                    for col in col_list:
                        fu = fu+1
                        str += tabledict[col]+'.'+col+','
                        temp = []
                        tab_name = tablenames[0]
                        ind = metadata[tab_name].index(col)
                        for data in tabledata[tablenames[0]]:
                            fu = fu+1
                            temp.append(data[ind])
                        col_dict[col]=temp
                    print(str[:-1])
                    t_length = len(col_dict[col])
                    for i in range(t_length):
                        str = ''
                        for col in col_list:
                            count1 = count1+1
                            str += col_dict[col][i]+','
                        str = str[:-1]
                        print(str)
                else:
                    str = ''
                    count1 = 0
                    col_id = 0
                    for col in col_list:
                        str += tabledict[col]+'.'+col+','
                    print(str[:-1])
                    # print(count1)
                    final_join = []
                    table_cell = 0
                    for table in tablenames:
                        temp = []
                        for i in tabledata[table]:
                            i = ','.join(i)
                            count1 = count1+1
                            temp.append(i)
                        final_join = joinTable1(final_join,temp)
                    count = 0
                    # print(count1)
                    ind_dict = defaultdict()
                    table_cell = table_cell + 1
                    for table in tablenames:
                        for i in metadata[table]:
                            ind_dict[i]=count
                            count = count+1
                    tr = 0
                    for line in final_join:
                        line = line.split(',')
                        str = ''
                        str2 = ''
                        for col in col_list:
                            col_id = col_id + 1
                            str+= line[ind_dict[col]]+','
                            
                        print(str[:-1])
        elif len(query) == 5:
            col_dict = defaultdict()
            rt = 4
            count1 = 0
            tablename = query[rt]
            col_list = query[rt-2].split( ',')
            col_absence = 0
            col_id = 0
            count4 = 0
            for col in col_list:
                try:
                    index = metadata[tablename].index(col)
                except:
                    print(col+"<- This column does not belong to the given table")
                    col_absence = col_absence + 1
                    sys.exit()
                temp = []
                for data in tabledata[tablename]:
                    count4 = count4 + 1
                    temp.append(data[index])
                col_dict[col] = temp
            str = ''
            str2 = ''
            for i in col_list:       str += tablename+'.'+i+','
            print(str[:-1])
            # print(count1)
            temp = []
            len_dict = len(col_dict[col])
            for i in range(len_dict):
                str = ''
                for col in col_list:
                    count1 = count1+1
                    str += col_dict[col][i]+','
                str = str[:-1]
                temp.append(str)
                count4 = count4 + 1
            temp = unique(temp)
            # print(count1)
            for i in temp: print(i)
        elif len(query)>6:
            dc = 1
            col_list = query[dc].split(',')
            tablenames = query[dc+2].split(',')
            tabledict = defaultdict()
            flag1=0
            flag2= 0
            id_len = 5
            tr = 0
            hall = 61
            for c in list(attribute.keys()):
                if query[id_len].find(c) != -1:
                    flag1 = 1
                    condition1 = query[id_len].split(c)[1]
                if query[id_len+2].find(c) != -1:
                    flag2 = 1
                    condition2 = query[id_len+2].split(c)[1]
            if flag1 == 0 or flag2 == 0:
                print('given column does not exist in the tables mentioned')
                tr = 1
                sys.exit()
            if ord(condition1[1]) == hall:
                condition1 = condition1[0] + condition1[1]
            else:
                condition1 = condition1[0]
            col1 = query[id_len].split(condition1)[0]
            value1 = query[id_len].split(condition1)[1]
            col_id1 = 0
            if ord(condition2[1]) == hall:
                condition2 = condition2[0] + condition2[1]
            else:
                condition2 = condition2[0]
            value2 = query[id_len+2].split(condition2)[1]
            col2 = query[id_len+2].split(condition2)[0]
            clist = [col1,col2]
            col_id4 = 0
            if query[1] != '*':
                col_id = 0
                tr = 0
                for col in col_list:
                    flag = 0
                    col_id = col_id + 1
                    for table in tablenames:
                        if col in metadata[table]:
                            col_id = col_id + 1
                            tabledict[col]=table
                            flag = 1
                    if flag == 0:
                        print('Given Columns not found in given table')
                        tr = 1
                        sys.exit()
                str = ''
                str2 = ''
                for col in col_list:
                    str += tabledict[col]+'.'+col+','
                    col_id = col_id+1
                print(str[:-1])
            else:
                col_id = 0
                tr = 0
                lines1 = []
                for c in clist:
                    flag = 0
                    for table in tablenames:
                        col_id = col_id + 1
                        if c in metadata[table]:
                            flag = 1
                    if flag == 0:
                        print('Given Column not found in the given table')
                        tr = 1
                        sys.exit()
                str = ''
                for table in tablenames:
                    for col in metadata[table]:
                        col_id = col_id + 1
                        str += table+'.'+col+','
                        lines1.append(str)

                print(str[:-1])
            final_join = []
            str2 = ''
            for table in tablenames:
                temp = []
                temp2 = []
                for i in tabledata[table]:
                    i = ','.join(i)
                    temp.append(i)
                    str2 = str2 + i
                    temp2.append(str2)
                final_join = joinTable1(final_join,temp)
            count = 0
            ind_dict = defaultdict()
            col_id = 0
            for table in tablenames:
                for i in metadata[table]:
                    ind_dict[i]=count
                    col_id = col_id + 1
                    count +=1
            if query[6] == 'AND':
                col_id = 0
                lines1 = []
                tr = 0
                if query[1] == '*':
                    try:
                        for line in final_join:
                            line = line.split(',')
                            lines1.append(line)
                        ops[condition1](line[ind_dict[col1]],value1) and ops[condition2](line[ind_dict[col2]],value2)
                    except:
                        print('Given column does not exist in the given tables')
                        tr = 1
                        sys.exit()
                    for line in final_join:
                        line = line.split(',')
                        col_id = col_id + 1
                        if ops[condition1](int(line[ind_dict[col1]]),int(value1)) and ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                            print((',').join(line))

                else:
                    str2 = ''
                    lines1=  []
                    for line in final_join:
                        line = line.split(',')
                        str2.append(line)
                        str = ''
                        try:
                            if ops[condition1](int(line[ind_dict[col1]]),int(value1)) and ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                                for col in col_list:
                                    str+= line[ind_dict[col]]+','
                                    col_id = col_id + 1
                                    lines1.append(str)
                                print(str[:-1])
                        except:
                            print('Given column does not exist in the given tables')
                            tr = 1
                            sys.exit()
            if query[6] == 'OR':
                col_id = 0
                lines1 = []
                tr = 0
                if query[1] == '*':
                    try:
                        for line in final_join:
                            line = line.split(',')
                            col_id = col_id + 1
                            lines1.append(line)
                        ops[condition1](line[ind_dict[col1]],value1) or ops[condition2](line[ind_dict[col2]],value2)
                    except:
                        print('Given column does not exist in the given tables')
                        tr = 1
                        sys.exit()
                    for line in final_join:
                        line = line.split(',')
                        col_id = col_id + 1
                        if ops[condition1](int(line[ind_dict[col1]]),int(value1)) or ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                            print((',').join(line))
                else:
                    for line in final_join:
                        line = line.split(',')
                        str = ''
                        try:
                            str2 = ''
                            if ops[condition1](int(line[ind_dict[col1]]),int(value1)) or ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                                for col in col_list:
                                    str+= line[ind_dict[col]]+','
                                    str2 = str2 + str
                                    lines1.append(str2)
                                print(str[:-1])
                        except:
                            print('Given column does not exist in the given tables')
                            tr = 1
                            sys.exit()
            elif query[6] != 'AND' and query[6] != 'OR':
                print('Only AND/OR operator allowed')
        elif len(query) == 6:
            check = 5
            id_col = 0
            tablenames[id_col] = query[check].split('=')[0].split('.')[0]
            tablenames[id_col+1] = query[check].split('=')[1].split('.')[0]
            col1 = query[check].split('=')[0].split('.')[1]
            col2 = query[check].split('=')[1].split('.')[1]
            # print(check+2)

            index2 = metadata[tablenames[1]].index(col2)
            index1 = metadata[tablenames[0]].index(col1)
            table1,table2 = [],[]
            tab_data = 0
            str2 = ''
            for i in range(len(tabledata[tablenames[1]])):
                temp = tabledata[tablenames[1]][i]
                tab_data = tab_data+1
                temp = ','.join(temp)
                str2 = str2 + temp
                table2.append(temp)
            
            str2 = ''
            for i in range(len(tabledata[tablenames[0]])):
                temp = tabledata[tablenames[0]][i]
                temp = ','.join(temp)
                str2 = str2 + temp
                tab_data = tab_data+1
                table1.append(temp)
            
            arr1,arr2 = [],[]
            arr2_len = 0
            arr1_len = 0
            # print(tab_data)
            for data in tabledata[tablenames[1]]:
                arr2.append(data[index2])
                arr2_len = arr2_len + 1
            
            for data in tabledata[tablenames[0]]:
                arr1_len = arr1_len + 1
                arr1.append(data[index1])
            
            finalans = []
            ans_count = 0
            if (query[1] == '*'):
                l_arr2 = len(arr2)
                for i in range(len(arr1)):
                    for j in range(l_arr2):
                        if arr2[j]==arr1[i]:
                            ans_count = ans_count + 1
                            finalans.append(table1[i]+','+table2[j])
                finalans = unique(finalans)
                str = ''
                tags1 = 0
                str2 = ''
                for tag in tablenames:
                    for key in metadata[tag]:
                        str2 = str2 + tag
                        str += tag+'.'+key+','
                        
                print(str[:-1])
                # print(ans_count)
                j= 0
                for i in finalans:
                    print(i)
                    j = i
                sys.exit()
            else:
                temp_dict = defaultdict()
                str = ''
                str2 = ''
                col_list = query[1].split(',')
                len_cell = len(col_list)
                tablenames = query[3].split(',')
                tr = 0
                for i in range(len_cell):
                    for j in range(len(tablenames)):
                        if col_list[i] in metadata[tablenames[j]]:
                            temp_dict[col_list[i]] = tablenames[j]
                            str += tablenames[j]+'.'+col_list[i]+','
                            tr+=1
                            break
                print(str[:-1])
                for i in range(len(arr1)):
                    col_id2 = 0
                    for j in range(len(arr2)):
                        if arr1[i] == arr2[j]:
                            str = ''
                            col_id2 = 0
                            for col in col_list:
                                index = metadata[temp_dict[col]].index(col)
                                if tablenames[0] == temp_dict[col]:
                                    str+= tabledata[temp_dict[col]][i][index]+','
                                    col_id2 = col_id2 + 1
                                else:
                                    str+= tabledata[temp_dict[col]][j][index]+','
                            print(str[:-1])

if __name__ == '__main__':
    
    attribute = attributeInfo('files/metadata.txt')
    tabledata = defaultdict()
    metadata = dbInfo('files/metadata.txt')
    meta1 = 0
    for values in metadata:
        meta1 =  meta1 + 1
        tabledata[values] = tableInfo(values,metadata[values])
    command = sys.argv[1]
    command = quotes_removal(command)
    if command[-1] != ';':
        print("Please put a ; at the end of the query")
        sys.exit()
    query1 =''
    query = command[:-1].strip()
    query2 = ''
    
    for i in range(len(query)):
        if query[i-1]== ',' and query[i]== ' ':
            continue
        if query[i]== ' ' and query[i+1]== ',':
            continue
        if query[i]== ' 'and query[i+1] in cond:
            continue
        if query[i-1] in cond and query[i]== ' ':
            continue
        else:
            query1= query1 + query[i]
    processQuery(query1)
