import random
import time
from datetime import date
import datetime
import threading


def createWarehouseTable(numWarehouses):
    warehouseTable = []
    for i in range(1, numWarehouses+1):
        warehouseTable.append({'W_ID': i,
                               'W_NAME': create_a_string(True, False, 6, 10),
                               'W_STREET_1': create_a_string(True, False, 10, 20),
                               'W_STREET_2': create_a_string(True, False, 10, 20),
                               'W_CITY': create_a_string(True, False, 10, 20),
                               'W_STATE': create_a_string(True, False, 1, 2),
                               'W_ZIP': create_zip(),
                               'W_TAX': round(random.uniform(0, 0.2), 4),
                               'W_YTD': round(float(300000), 2)})
    return warehouseTable


def createDistrictTable(numOfWarehouses):
    districtTable = []
    numDistricts = 10
    districtWarehouseNum = 1
    for _ in range(1, numOfWarehouses+1):
        for i in range(1, numDistricts+1):
            districtTable.append({'D_ID': i,
                                  'D_W_ID': districtWarehouseNum,
                                  'D_NAME': ('District ' + str(i)),
                                  'D_STREET_1': create_a_string(True, False, 10, 20),
                                  'D_STREET_2': create_a_string(True, False, 10, 20),
                                  'D_CITY': create_a_string(True, False, 10, 20),
                                  'D_STATE': create_a_string(True, False, 1, 2),
                                  'D_ZIP': create_zip(),
                                  'D_TAX': round(random.uniform(0, 0.2), 4),
                                  'D_YTD': round(float(30000), 2),
                                  'D_NEXT_O_ID': 3001})
        districtWarehouseNum += 1
    return districtTable


def create_last_name_run(lastNameList, NURand_num):
    randC_LAST = ''
    NURand_num = str(NURand_num)
    for num in NURand_num:
        if num == '0':
            randC_LAST += lastNameList[0]
        elif num == '1':
            randC_LAST += lastNameList[1]
        elif num == '2':
            randC_LAST += lastNameList[2]
        elif num == '3':
            randC_LAST += lastNameList[3]
        elif num == '4':
            randC_LAST += lastNameList[4]
        elif num == '5':
            randC_LAST += lastNameList[5]
        elif num == '6':
            randC_LAST += lastNameList[6]
        elif num == '7':
            randC_LAST += lastNameList[7]
        elif num == '8':
            randC_LAST += lastNameList[8]
        else:
            randC_LAST += lastNameList[9]
    return randC_LAST


def create_last_name(lastNameList, i):
    randC_LAST = ''
    if i < 1000:
        if i < 10:
            rand_num = '00' + str(i)
        elif i < 100:
            rand_num = '0' + str(i)
        else:
            rand_num = str(i)
    else:
        rand_num = str(NURand(255, 0, 999))
    for num in rand_num:
        if num == '0':
            randC_LAST += lastNameList[0]
        elif num == '1':
            randC_LAST += lastNameList[1]
        elif num == '2':
            randC_LAST += lastNameList[2]
        elif num == '3':
            randC_LAST += lastNameList[3]
        elif num == '4':
            randC_LAST += lastNameList[4]
        elif num == '5':
            randC_LAST += lastNameList[5]
        elif num == '6':
            randC_LAST += lastNameList[6]
        elif num == '7':
            randC_LAST += lastNameList[7]
        elif num == '8':
            randC_LAST += lastNameList[8]
        else:
            randC_LAST += lastNameList[9]
    return randC_LAST


def create_zip():
    randZIP = random.randint(0, 10000)
    if 100 >= randZIP < 1000:
        randZIP = '0' + str(randZIP) + '1111'
    if randZIP < 100:
        randZIP = '00' + str(randZIP) + '1111'
    return randZIP


def createCustomerTable(numOfWarehouses):
    customerTable = []
    numOfDistricts = 10
    numCustomers = 3000
    lastNameList = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
    customerWarehouseNum = 1
    customerDistrictNum = 1
    for k in range(1, numOfWarehouses+1):
        for j in range(1, numOfDistricts+1):
            for i in range(1, numCustomers+1):
                prob = random.randint(1, 100)
                if prob < 10:
                    customerTable.append({'C_ID': i,
                                      'C_D_ID': customerDistrictNum,
                                      'C_W_ID': customerWarehouseNum,
                                      'C_FIRST': create_a_string(True, False, 8, 16),
                                      'C_MIDDLE': 'OE',
                                      'C_LAST': create_last_name(lastNameList, i),
                                      'C_STREET_1': create_a_string(True, False, 10, 20),
                                      'C_STREET_2': create_a_string(True, False, 10, 20),
                                      'C_CITY': create_a_string(True, False, 10, 20),
                                      'C_STATE': create_a_string(True, False, 1, 2),
                                      'C_ZIP': create_zip(),
                                      'C_PHONE': create_n_string(0, 16),
                                      'C_SINCE': date.today().strftime("%d/%m/%Y"),
                                      'C_CREDIT': 'BC',
                                      'C_CREDIT_LIM': round(float(5000000), 2),
                                      'C_DISCOUNT': round(random.uniform(0, 0.5), 4),
                                      'C_BALANCE': round(float(-10), 2),
                                      'C_YTD_PAYMENT': round(float(10), 2),
                                      'C_PAYMENT_CNT': 1,
                                      'C_DELIVERY_CNT': 0,
                                      'C_DATA': create_a_string(True, False, 300, 500)})
                else:
                    customerTable.append({'C_ID': i,
                                          'C_D_ID': customerDistrictNum,
                                          'C_W_ID': customerWarehouseNum,
                                          'C_FIRST': create_a_string(True, False, 8, 16),
                                          'C_MIDDLE': 'OE',
                                          'C_LAST': create_last_name(lastNameList, i),
                                          'C_STREET_1': create_a_string(True, False, 10, 20),
                                          'C_STREET_2': create_a_string(True, False, 10, 20),
                                          'C_CITY': create_a_string(True, False, 10, 20),
                                          'C_STATE': create_a_string(True, False, 1, 2),
                                          'C_ZIP': create_zip(),
                                          'C_PHONE': create_n_string(0, 16),
                                          'C_SINCE': date.today().strftime("%d/%m/%Y"),
                                          'C_CREDIT': 'GC',
                                          'C_CREDIT_LIM': round(float(5000000), 2),
                                          'C_DISCOUNT': round(random.uniform(0, 0.5), 4),
                                          'C_BALANCE': round(float(-10), 2),
                                          'C_YTD_PAYMENT': round(float(10), 2),
                                          'C_PAYMENT_CNT': 1,
                                          'C_DELIVERY_CNT': 0,
                                          'C_DATA': create_a_string(True, False, 300, 500)})
                if i == 3000:
                    customerDistrictNum += 1
        customerWarehouseNum += 1
    return customerTable


def createHistoryTable(numOfWarehouses):
    historyTable = []
    numOfDistricts = 10
    numCustomers = 3000
    customerWarehouseNum = 1
    customerDistrictNum = 1
    for k in range(1, numOfWarehouses + 1):
        for j in range(1, numOfDistricts + 1):
            for i in range(1, numCustomers + 1):
                historyTable.append({'H_C_ID': i,
                                          'H_C_D_ID': customerDistrictNum,
                                          'H_C_W_ID': customerWarehouseNum,
                                          'H_DATE': date.today().strftime("%d/%m/%Y"),
                                          'H_AMOUNT': round(float(10), 2),
                                          'H_DATA': create_a_string(True, False, 12, 24)})

                if i == 3000:
                    customerDistrictNum += 1
        customerWarehouseNum += 1
    return historyTable


def addToHistoryTable(historyTable, H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA):
    historyTable.append({'H_C_ID': H_C_ID,
                         'H_C_D_ID': H_C_D_ID,
                         'H_C_W_ID': H_C_W_ID,
                         'H_D_ID': H_D_ID,
                         'H_W_ID': H_W_ID,
                         'H_DATE': H_DATE,
                         'H_AMOUNT': H_AMOUNT,
                         'H_DATA': H_DATA})


def createNewOrderTable():
    newOrderTable = []
    #num_order_lines = 900
    #for i in range(num_order_lines):
        #newOrderTable.append({'NO_O_ID': ,
         #                     'NO_D_ID': ,
          #                    'NO_W_ID': })
    return newOrderTable


def addNewOrder(newOrderTable, NO_O_ID, NO_D_ID, NO_W_ID):
    newOrderTable.append({'NO_O_ID': NO_O_ID,
                          'NO_D_ID': NO_D_ID,
                          'NO_W_ID': NO_W_ID})


def deleteNewOrder(newOrderTable, NO_O_ID, NO_D_ID, NO_W_ID):
    for newOrder in newOrderTable:
        if NO_O_ID == newOrder['NO_O_ID'] and NO_D_ID == newOrder['NO_D_ID'] and NO_W_ID == newOrder['NO_W_ID']:
            newOrderTable.remove(newOrder)


def createOrderTable():
    orderTable = []
    return orderTable


def addOrder(orderTable, O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL):
    orderTable.append({'O_ID': O_ID,
                       'O_D_ID': O_D_ID,
                       'O_W_ID': O_W_ID,
                       'O_C_ID': O_C_ID,
                       'O_ENTRY_D': O_ENTRY_D,
                       'O_CARRIER_ID': O_CARRIER_ID,
                       'O_OL_CNT': O_OL_CNT,
                       'O_ALL_LOCAL': O_ALL_LOCAL})


def createOrderLineTable():
    orderLineTable = []
    return orderLineTable


def addOrderLine(orderLineTable, OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D,
                 OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO):
    orderLineTable.append({'OL_O_ID': OL_O_ID,
                           'OL_D_ID': OL_D_ID,
                           'OL_W_ID': OL_W_ID,
                           'OL_NUMBER': OL_NUMBER,
                           'OL_I_ID': OL_I_ID,
                           'OL_SUPPLY_W_ID': OL_SUPPLY_W_ID,
                           'OL_DELIVERY_D': OL_DELIVERY_D,
                           'OL_QUANTITY': OL_QUANTITY,
                           'OL_AMOUNT': OL_AMOUNT,
                           'OL_DIST_INFO': OL_DIST_INFO})


def create_a_string(other, data, beg, end):
    #alpha = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
             #'v', 'w', 'x', 'y', 'z']
    if other is True:
        #a_string = ''
        a_string_num = random.randint(beg, end)
        a_string = a_string_num * 'a'
        #for i in range(a_string_num):
        #    letter_index = random.randint(0, (len(alpha)-1))
        #    a_string += alpha[letter_index]
        return a_string
    if data is True:
        prob = random.randint(0, 100)
        #a_string = ''
        a_string_data = random.randint(beg, end)
        #for i in range(a_string_data):
        #    letter_index = random.randint(0, (len(alpha)-1))
        #    a_string += alpha[letter_index]
        a_string = a_string_data * 'a'
        if prob < 10:
            ins_index = random.randint(0, (len(a_string)-1))
            a_string = a_string[0:ins_index] + 'ORIGINAL' + a_string[ins_index:-1]
        return a_string


def create_zip():
    randZIP = random.randint(0, 10000)
    if 100 >= randZIP < 1000:
        randZIP = '0' + str(randZIP) + '1111'
    elif randZIP < 100:
        randZIP = '00' + str(randZIP) + '1111'
    else:
        randZIP = randZIP
    return randZIP


def create_n_string(beg, end):
    n_string = ''
    for i in range(beg, end):
        rand_int = random.randint(0, 9)
        n_string += str(rand_int)
    return n_string


def createItemTable():
    prob = random.randint(0, 100)
    itemTable = []
    itemNum = 100000
    for i in range(1, itemNum+1):
        if prob < 10:
            itemTable.append({'I_ID': i,
                            'I_IM_ID': random.randint(1, 10001),
                            'I_NAME': create_a_string(True, False, 14, 24),
                            'I_PRICE': round(random.uniform(100, 1000), 2),
                            'I_DATA': create_a_string(False, True, 26, 50)})
        else:
            itemTable.append({'I_ID': i,
                              'I_IM_ID': random.randint(1, 10001),
                              'I_NAME': create_a_string(True, False, 14, 24),
                              'I_PRICE': round(random.uniform(100, 1000), 2),
                              'I_DATA': create_a_string(False, True, 26, 50)})
    return itemTable


def createStockTable(numWarehouse):
    stockTable = []
    numStocks = 100000
    stockWarehouseNum = 1
    for _ in range(1, numWarehouse+1):
        for i in range(1, numStocks+1):
            stockTable.append({'S_I_ID': i,
                               'S_W_ID': stockWarehouseNum,
                               'S_QUANTITY': random.randint(10, 100),
                               'S_DIST_01': create_a_string(True, False, 0, 24),
                               'S_DIST_02': create_a_string(True, False, 0, 24),
                               'S_DIST_03': create_a_string(True, False, 0, 24),
                               'S_DIST_04': create_a_string(True, False, 0, 24),
                               'S_DIST_05': create_a_string(True, False, 0, 24),
                               'S_DIST_06': create_a_string(True, False, 0, 24),
                               'S_DIST_07': create_a_string(True, False, 0, 24),
                               'S_DIST_08': create_a_string(True, False, 0, 24),
                               'S_DIST_09': create_a_string(True, False, 0, 24),
                               'S_DIST_10': create_a_string(True, False, 0, 24),
                               'S_YTD': 0,
                               'S_ORDER_CNT': 0,
                               'S_REMOTE_CNT': 0,
                               'S_DATA': create_a_string(False, True, 26, 50)})
        stockWarehouseNum += 1
    return stockTable


# Calculate nonuniform random number for C_LAST, C_ID, or OL_I_ID
def NURand(A, x, y):
    C = random.randint(0, A)
    result = (((random.randint(0, A) | random.randint(x, y)) + C) % (y - x + 1)) + x
    return result


def newOrderTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                        warehouseTable, districtTable, customerTable, itemTable, newOrderTable, orderLineTable,
                        orderTable, stockTable, sender_mem):
    stockTableInitial = stockTable.copy()
    # ------------------------------------------------------------------------------------------------------------------
    # Getting information from wareHouseTable, districtTable, customerTable
    # ------------------------------------------------------------------------------------------------------------------
    for warehouse in warehouseTable:
        if W_ID == warehouse['W_ID']:
            W_TAX = warehouse['W_TAX']
            break

    for district in districtTable:
        if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
            D_TAX = district['D_TAX']
            D_NEXT_O_ID = district['D_NEXT_O_ID']
            district['D_NEXT_O_ID'] += 1
            break

    customerInfo = []
    for customer in customerTable:
        if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
            C_DISCOUNT = customer['C_DISCOUNT']
            C_LAST = customer['C_LAST']
            C_CREDIT = customer['C_CREDIT']
            customerInfo.append([C_DISCOUNT, C_LAST, C_CREDIT])
            break

    # ------------------------------------------------------------------------------------------------------------------
    # Adding the order to the orderTable and newOrderTable
    # ------------------------------------------------------------------------------------------------------------------

    # determines whether all of the items in I_IDS are from the same warehouse where a list of warehouse IDs respective
    #   to an item is compared to the warehouse ID: I_W_IDS = W_ID

    # also adds all item information (item price, item name, item data) to a list of items

    # O_OL_CNT is ol_cnt which is the total number of items in the order
    O_ALL_LOCAL = True
    items = []
    for i in range(len(I_IDS)):
        O_ALL_LOCAL = O_ALL_LOCAL and I_W_IDS[i] == W_ID
        for item in itemTable:
            if I_IDS[i] == item['I_ID']:
                items.append([item['I_PRICE'], item['I_NAME'], item['I_DATA']])
                badItemNumFlag = False
                break
            else:
                badItemNumFlag = True
        if badItemNumFlag == True:
            items.append([])

    O_CARRIER_ID = None
    O_OL_CNT = len(I_IDS)
    addNewOrder(newOrderTable, D_NEXT_O_ID, D_ID, W_ID)
    addOrder(orderTable, D_NEXT_O_ID, D_ID, W_ID, C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)

    # ------------------------------------------------------------------------------------------------------------------
    # Retrieve information from stockTable, update quantity and order and remote counts
    # ------------------------------------------------------------------------------------------------------------------
    itemInfo = []
    TOTAL_AMOUNT = 0
    for i in range(O_OL_CNT):
        # Fixed 1% of the New-Order Transaction are chosen at random to simulate data errors, determined based off of
        #   rbk input where if rbk is set to 1, then an invalid item value is placed into I_IDS
        if len(items[i]) == 0:
            # recover the old values for stockTable and exit the benchmark with item number is not valid
            for stock, stockI in zip(stockTable, stockTableInitial):
                stock['S_QUANTITY'] = stockI['S_QUANTITY']
                stock['S_YTD'] = stockI['S_YTD']
                stock['S_ORDER_CNT'] = stockI['S_ORDER_CNT']
                stock['S_REMOTE_CNT'] = stockI['S_REMOTE_CNT']
            return 'Item number is not valid'

        OL_NUMBER = i + 1
        OL_SUPPLY_W_ID = I_W_IDS[i]
        OL_I_ID = I_IDS[i]
        OL_QUANTITY = I_QTYS[i]
        itemInfo = items[i]
        I_PRICE = itemInfo[0]
        I_NAME = itemInfo[1]
        I_DATA = itemInfo[2]
        for stock in stockTable:
            if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                S_QUANTITY = stock['S_QUANTITY']
                S_YTD = stock['S_YTD']
                S_ORDER_CNT = stock['S_ORDER_CNT']
                S_REMOTE_CNT = stock['S_REMOTE_CNT']
                S_DATA = stock['S_DATA']
                S_DIST_xx = ['S_DIST_' + '{0:0=2d}'.format(D_ID)]
                break
        if S_QUANTITY >= OL_QUANTITY + 10:
            S_QUANTITY -= OL_QUANTITY
        else:
            S_QUANTITY = (S_QUANTITY - OL_QUANTITY) + 91
        S_YTD += OL_QUANTITY
        S_ORDER_CNT += 1
        if OL_SUPPLY_W_ID != W_ID:
            S_REMOTE_CNT += 1

        # Updating stock table
        for stock in stockTable:
            if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                stock['S_QUANTITY'] = S_QUANTITY
                stock['S_YTD'] = S_YTD
                stock['S_ORDER_CNT'] = S_ORDER_CNT
                stock['S_REMOTE_CNT'] = S_REMOTE_CNT
                break

        OL_AMOUNT = OL_QUANTITY * I_PRICE
        TOTAL_AMOUNT += OL_AMOUNT

        if I_DATA.find("ORIGINAL") != -1 and S_DATA.find("ORIGINAL") != -1:
            BRAND_GENERIC = 'B'
        else:
            BRAND_GENERIC = 'G'

        addOrderLine(orderLineTable, D_NEXT_O_ID, D_ID, W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, O_ENTRY_D, OL_QUANTITY, OL_AMOUNT,
                     S_DIST_xx)

        itemInfo.append([I_NAME, S_QUANTITY, BRAND_GENERIC, I_PRICE, OL_AMOUNT])

    TOTAL_AMOUNT *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
    otherInfo = [(W_TAX, D_TAX, D_NEXT_O_ID, TOTAL_AMOUNT)]
    return [customerInfo, otherInfo, itemInfo]


def paymentTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE, warehouseTable, districtTable,
                       customerTable, historyTable):
    warehouse_info = []
    district_info = []
    customer_info = []
    for warehouse in warehouseTable:
        if W_ID == warehouse['W_ID']:
            W_NAME = warehouse['W_NAME']
            W_STREET_1 = warehouse['W_STREET_1']
            W_STREET_2 = warehouse['W_STREET_2']
            W_CITY = warehouse['W_CITY']
            W_STATE = warehouse['W_STATE']
            W_ZIP = warehouse['W_ZIP']
            warehouse['W_YTD'] = warehouse['W_YTD'] + H_AMOUNT
            warehouse_info.append([W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, warehouse['W_YTD']])
            break
    for district in districtTable:
        if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
            D_NAME = district['D_NAME']
            D_STREET_1 = district['D_STREET_1']
            D_STREET_2 = district['D_STREET_2']
            D_CITY = district['D_CITY']
            D_STATE = district['D_STATE']
            D_ZIP = district['D_ZIP']
            district['D_YTD'] = district['D_YTD'] + H_AMOUNT
            district_info.append([D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, district['D_YTD']])
            break
    if C_ID is None:
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                C_STREET_1 = customer['C_STREET_1']
                C_STREET_2 = customer['C_STREET_2']
                C_CITY = customer['C_CITY']
                C_STATE = customer['C_STATE']
                C_ZIP = customer['C_ZIP']
                C_PHONE = customer['C_PHONE']
                C_SINCE = customer['C_SINCE']
                C_CREDIT = customer['C_CREDIT']
                C_CREDIT_LIM = customer['C_CREDIT_LIM']
                C_DISCOUNT = customer['C_DISCOUNT']
                C_DATA = customer['C_DATA']
                customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                C_BALANCE = customer['C_BALANCE']
                customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                C_YTD_PAYMENT = customer['C_YTD_PAYMENT']
                customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                C_PAYMENT_CNT = customer['C_PAYMENT_CNT']
                customer_info.append([C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                                     C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, C_BALANCE,
                                     C_YTD_PAYMENT, C_PAYMENT_CNT])
    else:
        customer_with_lastname = []
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                customer_with_lastname.append(customer)
        # sorts the list of customers in ascending order based off of C_FIRST
        sorted(customer_with_lastname, key=lambda i: i['C_FIRST'])
        # gets n/2 positioned customer where C_LAST is all the same
        customer_count = len(customer_with_lastname)
        index = int((customer_count-1)/2)
        picked_customer = customer_with_lastname[index]
        C_ID = picked_customer['C_ID']
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_ID = customer['C_ID']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_STREET_1 = customer['C_STREET_1']
                C_STREET_2 = customer['C_STREET_2']
                C_CITY = customer['C_CITY']
                C_STATE = customer['C_STATE']
                C_ZIP = customer['C_ZIP']
                C_PHONE = customer['C_PHONE']
                C_SINCE = customer['C_SINCE']
                C_CREDIT = customer['C_CREDIT']
                C_CREDIT_LIM = customer['C_CREDIT_LIM']
                C_DISCOUNT = customer['C_DISCOUNT']
                C_DATA = customer['C_DATA']
                customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                customer_info.append([C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                                     C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, customer['C_BALANCE'],
                                     customer['C_YTD_PAYMENT'], customer['C_PAYMENT_CNT']])
    if C_CREDIT == 'BC':
        # map will perform the string typecasting on all of the iterables in the list, then join the strings together
        new_c_data = " ".join(map(str, [C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_AMOUNT]))
        C_DATA = (new_c_data + C_DATA)
        if len(C_DATA) > 500:
            #list slicing, will ensure that all characters to the right of 500 chars are removed
            C_DATA = C_DATA[:500]
    H_DATA = W_NAME + '    ' + D_NAME
    addToHistoryTable(historyTable, C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_DATE, H_AMOUNT, H_DATA)
    return [warehouse_info, district_info, customer_info]


def orderStatusTransaction(C_W_ID, C_D_ID, C_ID, C_LAST, customerTable, orderTable, orderLineTable):
    customer_info = []
    if C_ID != None:
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_BALANCE = customer['C_BALANCE']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
    else:
        customer_with_lastname = []
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                customer_with_lastname.append(customer)
        # sorts the list of customers in ascending order based off of C_FIRST
        sorted(customer_with_lastname, key=lambda i: i['C_FIRST'])
        # gets n/2 positioned customer where C_LAST is all the same
        customer_count = len(customer_with_lastname)
        index = int((customer_count-1)/2)
        picked_customer = customer_with_lastname[index]
        C_ID = picked_customer['C_ID']
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_BALANCE = customer['C_BALANCE']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
    ordertable_info = []
    O_W_ID = C_W_ID
    O_D_ID = C_D_ID
    O_C_ID = C_ID
    # TODO: Figure out what needs to be done if table is initially empty
    # for now set O_ID to None if orderTable is empty
    O_ID = None
    for order in orderTable:
        if O_W_ID == order['O_W_ID'] and O_D_ID == order['O_D_ID'] and O_C_ID == order['O_C_ID']:
            O_ID = order['O_ID']
            O_ENTRY_D = order['O_ENTRY_D']
            O_CARRIER_ID = order['O_CARRIER_ID']
            ordertable_info.append([O_ID, O_ENTRY_D, O_CARRIER_ID])
    orderlinetable_info = []
    OL_W_ID = O_W_ID
    OL_D_ID = O_D_ID
    OL_O_ID = O_ID
    for orderLine in orderLineTable:
        if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_W_ID'] and OL_O_ID == orderLine['OL_O_ID']:
            OL_I_ID = orderLine['OL_I_ID']
            OL_SUPPLY_W_ID = orderLine['OL_SUPPLY_W_ID']
            OL_QUANTITY = orderLine['OL_QUANTITY']
            OL_AMOUNT = orderLine['OL_AMOUNT']
            OL_DELIVERY_D = orderLine['OL_DELIVERY_D']
            orderlinetable_info.append([OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D])
    return [customer_info, ordertable_info, orderlinetable_info]


def deliveryTransaction(W_ID, O_CARRIER_ID, OL_DELIVERY_D, customerTable, newOrderTable, orderTable, orderLineTable):
    result_file = []
    for D_ID in range(1, 11):
        NO_W_ID = W_ID
        NO_D_ID = D_ID
        low_NO_O_ID = float('inf')
        for newOrder in newOrderTable:
            if NO_W_ID == newOrder['NO_W_ID'] and NO_D_ID == newOrder['NO_D_ID'] and newOrder['NO_O_ID'] < low_NO_O_ID:
                low_NO_O_ID = newOrder['NO_O_ID']
        if low_NO_O_ID == float('inf'):
            #TODO: if transaction is skipped, and skipped amount of transactions is more than 1% of total transactions,
            # then must report this. The ending result file produced should show percentage of skipped deliveries and
            # skipped districts
            continue
        deleteNewOrder(newOrderTable, low_NO_O_ID, NO_D_ID, NO_W_ID)
        O_W_ID = W_ID
        O_D_ID = D_ID
        O_ID = low_NO_O_ID
        for order in orderTable:
            if O_W_ID == order['O_W_ID'] and O_D_ID == order['O_D_ID'] and O_ID == order['O_ID']:
                O_C_ID = order['O_C_ID']
                order['O_CARRIER_ID'] = O_CARRIER_ID
        OL_W_ID = O_W_ID
        OL_D_ID = O_D_ID
        OL_O_ID = O_ID
        sum_total = 0
        for orderLine in orderLineTable:
            if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and OL_O_ID == orderLine['OL_O_ID']:
                orderLine['OL_DELIVERY_D'] = OL_DELIVERY_D
                sum_total += orderLine['OL_AMOUNT']
        C_W_ID = W_ID
        C_D_ID = D_ID
        C_ID = O_C_ID
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID  == customer['C_ID']:
                customer['C_BALANCE'] += sum_total
                customer['C_DELIVERY_CNT'] += 1
        result_file.append((D_ID, low_NO_O_ID))
    return result_file


def stockLevelTransaction(W_ID, D_ID, threshold, districtTable, orderLineTable, stockTable):
    low_stock = 0
    D_W_ID = W_ID
    for district in districtTable:
        if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
            D_NEXT_O_ID = district['D_NEXT_O_ID']
    OL_W_ID = W_ID
    OL_D_ID = D_ID
    for orderLine in orderLineTable:
        if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and \
                (orderLine['OL_O_ID'] < D_NEXT_O_ID or orderLine['OL_O_ID'] >= (D_NEXT_O_ID - 20)):
            S_I_ID = orderLine['OL_I_ID']
            S_W_ID = W_ID
            for stock in stockTable:
                if S_I_ID == stock['S_I_ID'] and S_W_ID == stock['S_W_ID'] and stock['S_QUANTITY'] < threshold:
                    low_stock += 1
    return low_stock


'''
# Number of Warehouses
numOfWarehouses = 1

# Create Tables
warehouseTable = createWarehouseTable(numOfWarehouses)
districtTable = createDistrictTable(numOfWarehouses)
customerTable = createCustomerTable(numOfWarehouses)
historyTable = createHistoryTable(numOfWarehouses)
newOrderTable = createNewOrderTable()
orderTable = createOrderTable()
orderLineTable = createOrderLineTable()
itemTable = createItemTable()
stockTable = createStockTable(numOfWarehouses)


# Create input parameters for newOrderTransaction
W_ID = 1
D_ID = random.randint(1, 10)
C_ID = NURand(1023, 1, 3000)
ol_cnt = random.randint(5, 15)
rbk = random.randint(1, 100)
I_IDS = []
I_W_IDS = []
I_QTYS = []
for item in range(ol_cnt):
    if item == ol_cnt-1 and rbk == 1:
        OL_I_ID = 3000000
    else:
        OL_I_ID = NURand(8191, 1, 100000)
    I_IDS.append(OL_I_ID)
    x = random.randint(1, 100)
    if x > 1:
        OL_SUPPLY_W_ID = W_ID
    if x == 1:
        list_W_IDS = []
        for i in range(1, numOfWarehouses+1):
            list_W_IDS.append(i)
        list_W_IDS.remove(W_ID)
        OL_SUPPLY_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS)-1))]
    I_W_IDS.append(OL_SUPPLY_W_ID)
    OL_QUANTITY = random.randint(1, 10)
    I_QTYS.append(OL_QUANTITY)
O_ENTRY_D = date.today().strftime("%d/%m/%Y")

# W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS

start = time.process_time()
result = newOrderTransaction(W_ID, W_ID, D_ID, W_ID, D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                    warehouseTable, districtTable, customerTable, itemTable, newOrderTable, orderLineTable, orderTable, stockTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')


# Create input parameters for paymentTransaction
W_ID = 1
D_W_ID = W_ID
D_ID = random.randint(1, 10)
x = random.randint(1, 100)
y = random.randint(1, 100)
if x <= 85:
    C_D_ID = D_ID
    C_W_ID = W_ID
else:
    C_D_ID = random.randint(1, 10)
    C_W_ID = random.randint(1, numOfWarehouses)
if y <= 60:
    lastNameList = ['Robinson', 'Nguyen', 'Juarez']
    C_LAST = lastNameList[NURand(255, 0, 999) % len(lastNameList)]
    C_ID = None
else:
    C_LAST = None
    C_ID = NURand(1023, 1, 3000)
print(C_LAST)
H_AMOUNT = round(random.uniform(1, 5000000), 2)
H_DATE = date.today().strftime("%d/%m/%Y")
start = time.process_time()
result = paymentTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE, warehouseTable, districtTable,
                   customerTable, historyTable)
end = time.process_time()
print(result)
print(str(end - start) + ' sec')


# Create input parameters for orderStatusTransaction
W_ID = 1
D_ID = random.randint(1, 10)
y = random.randint(1, 100)
if y <= 60:
    lastNameList = ['Robinson', 'Nguyen', 'Juarez']
    C_LAST = lastNameList[NURand(255, 0, 999) % len(lastNameList)]
    C_ID = None
else:
    C_LAST = None
    C_ID = NURand(1023, 1, 3000)
start = time.process_time()
result = orderStatusTransaction(W_ID, D_ID, C_ID, C_LAST, customerTable, orderTable, orderLineTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')


# Create input parameters for deliveryTransaction
W_ID = 1
O_CARRIER_ID = random.randint(1, 10)
OL_DELIVERY_D = date.today().strftime("%d/%m/%Y")
start = time.process_time()
result = deliveryTransaction(W_ID, O_CARRIER_ID, OL_DELIVERY_D, customerTable, newOrderTable, orderTable, orderLineTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')


# Create input parameters for stockLevelTransaction
W_ID = 1
D_ID = 1
threshold = random.randint(10, 20)
start = time.process_time()
result = stockLevelTransaction(W_ID, D_ID, threshold, districtTable, orderLineTable, stockTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')
'''
