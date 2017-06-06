#!/usr/bin/env bash

FECHA=201505

LOCAL_PATH_PREFIX={{ remote.inbox }}

SRC_PATH=${LOCAL_PATH_PREFIX}/ESP/LTV
TRG_PATH=${LOCAL_PATH_PREFIX}/ESP/LTV

FICHEROS_FTP="
  _ANTIG_TERM.rar
  _CANJES.rar
  _CONT_COMPROMISO.rar
  _CONTRATOS.rar
  _ESTADOS_LINEAS.rar
  _INF_LIN_TERM1.rar
  _INF_LINEA_VALOR_EM.rar
  _INF_LINEA_VALOR.rar
  _LINEAS_MULTISIM.rar
  _LINEAS_SERVICIOS.rar
  _MOV_DESC_TERM.rar
  _MOV_TERM.rar
  _MOVIMIENTOS_SERVICIOS.rar
  _POBLACIONES.rar
  _SEGMENTO_ORGANIZATIVO.rar
  _SEGMENTOS_TERM.rar
  _TERM_VOZDATOS1.rar
  _TERM_VOZDATOS2.rar
  _TRAF_VOZ_HRC.rar
  A_IMEISHIST.rar
  B_IMEISHIST.rar
"

cd ${SRC_PATH} > /dev/null 2>&1

rm -fr script-${FECHA}.ftp
echo "cd ESP/LTV"                        > script-${FECHA}.ftp
for FICH_FTP in ${FICHEROS_FTP}; do
  echo "get ${FECHA}${FICH_FTP} ${SRC_PATH}" >> script-${FECHA}.ftp
done

echo -e "\nInvoca al cliente sftp con:\n"
echo sudo -u hdfs sftp -oPort=2222 test@10.93.12.220
echo -e "\nEjecuta lo siguiente:"
cat script-${FECHA}.ftp
echo -e "\nY cuando termine, ejecuta sudo -u hdfs ./ingest-ESP-files.sh ${FECHA} ${LOCAL_PATH_PREFIX}"
