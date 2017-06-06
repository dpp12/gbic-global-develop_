#!/usr/bin/env bash

FECHA=201506

LOCAL_PATH_PREFIX={{ remote.inbox }}

SRC_PATH=${LOCAL_PATH_PREFIX}/BRA/LTV
TRG_PATH=${LOCAL_PATH_PREFIX}/BRA/LTV

FICHEROS_FTP="
  201_CUSTOMER_
  201_DIM_CUSTOMER_
  201_DIM_SERVICES_
  201_INF_LINE_DEVICE_
  CUST_
  DIM_DIRECAO_CHAMADA_
  DIM_MOV_TYPE_
  DIM_PLATAFORMA_
  DIM_PLNO_
  DIM_REGION_
  DIM_SEGMENT_
  DIM_SENTIDO_CHAMADA_
  DIM_SENTIDO_COBRANCA_
  DIM_SIST_PAGAMENTO_
  DIM_SITU_CHAMADO_CHAMADOR_
  DIM_TIPO_COBRANCA_
  DIM_TIPO_TRAFEGO_
  FATURA_
  INF_LINE_DEVICE_
  INTERCON_
  LINE_SERVICES_
  MOV_DEV_
  RECARGA_
  TRAFEGO_
  TRAFEGO_DADOS_
"

cd ${SRC_PATH} > /dev/null 2>&1

rm -fr script-${FECHA}.ftp
echo "cd BRA/LTV"                      > script-${FECHA}.ftp
for FICH_FTP in ${FICHEROS_FTP}; do
  echo "get ${FECHA}${FICH_FTP}.rar ${SRC_PATH}" >> script-${FECHA}.ftp
done

echo -e "\nInvoca al cliente sftp con:\n"
echo sudo -u hdfs sftp -oPort=2222 test@10.93.12.220
echo -e "\nEjecuta lo siguiente:"
cat script-${FECHA}.ftp
echo -e "\nY cuando termine, ejecuta sudo -u hdfs ./ingest-BRA-files.sh ${FECHA} ${LOCAL_PATH_PREFIX}"
