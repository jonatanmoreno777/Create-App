# -*- coding: utf-8 -*-
"""
Created on Mon Oct 13 22:09:49 2025

@author: ASUS
"""

import requests
import json
import re
import pandas as pd
from datetime import datetime
import time
import os

class SNIRHDownloader:
    def __init__(self, download_folder="D:\\G"):
        self.session = requests.Session()
        self.download_folder = download_folder
        self.setup_headers()
        os.makedirs(self.download_folder, exist_ok=True)
        print(f"📁 Carpeta de descargas: {self.download_folder}")
    
    def setup_headers(self):
        self.session.headers.update({
            'Content-Type': 'application/json; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://snirh.ana.gob.pe/ANDREA/Integrado.aspx',
            'Origin': 'https://snirh.ana.gob.pe',
            'X-Requested-With': 'XMLHttpRequest'
        })
    
    def clean_name(self, text):
        if not text: return ""
        clean_text = re.sub(r'<[^>]+>', '', text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        clean_text = re.sub(r'[\\/*?:"<>|]', '', clean_text)
        return clean_text
    
    def get_basins(self):
        """Obtener lista de cuencas disponibles"""
        url = "https://snirh.ana.gob.pe/ANDREA/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": "", "pBuscar": "", "pTipo": "UH"}
        try:
            print("📋 Obteniendo cuencas...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                cuencas = json.loads(response.json()['d'])
                print(f"✅ Cuencas obtenidas: {len(cuencas)}")
                return cuencas
            return []
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def get_stations_by_basin(self, codigo_cuenca, categoria=None):
        """Obtener estaciones de una cuenca"""
        url = "https://snirh.ana.gob.pe/ANDREA/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": codigo_cuenca, "pBuscar": "", "pTipo": "EST"}
        try:
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                estaciones = json.loads(response.json()['d'])
                if categoria:
                    estaciones = [e for e in estaciones if e.get('CAT') == categoria]
                return estaciones
            return []
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def get_station_variables(self, id_estacion):
        """Obtener variables de una estación"""
        url = "https://snirh.ana.gob.pe/ANDREA/index.aspx/VariableListarxEstacion"
        data = {"pIDEstacion": id_estacion, "pTipo": "DMA"}
        try:
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                resultado = response.json()
                if 'd' in resultado:
                    variables = json.loads(resultado['d'])
                    return variables
            return []
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def download_daily_data(self, id_estacion, variable_info):
        """Descargar datos diarios"""
        url = "https://snirh.ana.gob.pe/ANDREA/ServicioGeneral.asmx/SerieDatos"
        data = {
            "IDEstacion": id_estacion,
            "IDOperador": variable_info.get('IDOPERADOR', ''),
            "IDVariable": variable_info.get('ID', ''),
            "TipoSerie": "DIARIA",
            "pIDEstudio": variable_info.get('IDESTUDIO', ''),
            "IDUsuario": 0,
            "IDTipoRegistro": 1
        }
        try:
            print(f"📥 Descargando datos diarios...")
            response = self.session.post(url, json=data, timeout=60)
            if response.status_code == 200:
                resultado = response.json()
                if 'd' in resultado and resultado['d']:
                    serie_datos = resultado['d']
                    print(f"✅ Datos diarios descargados: {len(serie_datos)} registros")
                    return serie_datos
                else:
                    print("❌ No se encontraron datos en la respuesta")
                    return []
            else:
                print(f"❌ Error HTTP: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def _process_date(self, fecha):
        """Convertir fecha a datetime"""
        if isinstance(fecha, str):
            formatos = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
            for fmt in formatos:
                try: 
                    return datetime.strptime(fecha.strip(), fmt)
                except ValueError: 
                    continue
        return fecha
    
    def _convert_number(self, valor):
        """Convertir valor a número - FUNCIÓN CORREGIDA"""
        if valor is None or valor == '':
            return None
        try:
            if isinstance(valor, str):
                valor = valor.strip().replace(',', '.')
                if valor in ['', 'NA', 'N/A', 'NULL']:
                    return None
            return float(valor)
        except (ValueError, TypeError):
            return None
    
    def process_daily_data(self, serie_datos):
        """Procesar datos diarios - SOLO FECHA SIN HORA"""
        if not serie_datos: 
            return []
        
        datos_procesados = []
        
        for item in serie_datos:
            if isinstance(item, list) and len(item) >= 2:
                fecha = item[0]
                valor = item[1]
                
                # Procesar fecha
                fecha_dt = self._process_date(fecha)
                
                # SOLO FECHA, SIN HORA
                if isinstance(fecha_dt, datetime):
                    fecha_sola = fecha_dt.strftime('%Y-%m-%d')
                else:
                    fecha_sola = str(fecha_dt)
                
                fila = {
                    'FECHA': fecha_sola,
                    'AÑO': fecha_dt.year if isinstance(fecha_dt, datetime) else None,
                    'MES': fecha_dt.month if isinstance(fecha_dt, datetime) else None,
                    'DIA': fecha_dt.day if isinstance(fecha_dt, datetime) else None,
                    'VALOR': self._convert_number(valor)  # ← CORREGIDO
                }
                datos_procesados.append(fila)
        
        print(f"📅 Fechas procesadas: {len(datos_procesados)} registros")
        return datos_procesados
    
    def download_station_data(self, id_estacion, nombre_estacion, categoria):
        """Descargar datos de una estación"""
        print(f"\n🎯 Procesando estación: {nombre_estacion}")
        print(f"🆔 ID: {id_estacion}")
        if categoria:
            print(f"📂 Categoría: {categoria}")
        print("-" * 40)
        
        # Obtener variables
        variables = self.get_station_variables(id_estacion)
        if not variables: 
            print("❌ No se encontraron variables")
            return None
        
        print(f"📊 Variables encontradas: {len(variables)}")
        
        resultados = {}
        
        for i, variable in enumerate(variables):
            nombre_var = variable.get('NOMBRE', '')
            unidad = variable.get('UNIDADMEDIDA', '')
            
            print(f"\n📋 Variable {i+1}/{len(variables)}: {nombre_var}")
            print(f"   📏 Unidad: {unidad}")
            
            # Procesar TODAS las variables diarias (no solo "1DIA")
            if "DIA" in nombre_var.upper() or "DIAR" in nombre_var.upper():
                print(f"   🔍 Procesando datos diarios...")
                
                # Descargar datos
                serie_datos = self.download_daily_data(id_estacion, variable)
                
                if serie_datos:
                    # Procesar datos
                    datos_procesados = self.process_daily_data(serie_datos)
                    
                    if datos_procesados:
                        clave = f"{nombre_var}"
                        resultados[clave] = {
                            'datos': datos_procesados,
                            'unidad': unidad,
                            'estadisticas': {
                                'total_registros': len(datos_procesados),
                                'valores_no_nulos': sum(1 for d in datos_procesados if d['VALOR'] is not None)
                            }
                        }
                        
                        stats = resultados[clave]['estadisticas']
                        print(f"   ✅ {stats['total_registros']} registros ({stats['valores_no_nulos']} con valores)")
                    else:
                        print("   ❌ No se pudieron procesar los datos")
                else:
                    print("   ❌ No se obtuvieron datos")
            else:
                print(f"   ⏭️  Saltando (solo datos diarios)")
            
            # Pausa entre variables
            time.sleep(1)
        
        return resultados if resultados else None
    
    def save_to_csv(self, resultados, id_estacion, nombre_estacion, categoria=""):
        """Guardar SOLO CSV"""
        if not resultados: 
            print("❌ No hay datos para guardar")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_limpio = self.clean_name(nombre_estacion).replace(' ', '_')[:40]
        
        # Nombre del archivo
        if categoria:
            nombre_archivo = f"SNIRH_{id_estacion}_{nombre_limpio}_{categoria}_{timestamp}.csv"
        else:
            nombre_archivo = f"SNIRH_{id_estacion}_{nombre_limpio}_{timestamp}.csv"
        
        ruta_csv = os.path.join(self.download_folder, nombre_archivo)
        
        try:
            # Combinar todos los datos
            todos_datos = []
            for nombre_var, datos in resultados.items():
                for registro in datos['datos']:
                    registro['VARIABLE'] = nombre_var
                    registro['UNIDAD'] = datos['unidad']
                    todos_datos.append(registro)
            
            df = pd.DataFrame(todos_datos)
            
            # Ordenar por fecha
            if 'FECHA' in df.columns:
                df = df.sort_values('FECHA')
            
            # ORDEN DE COLUMNAS
            orden_columnas = ['FECHA', 'AÑO', 'MES', 'DIA', 'VALOR', 'VARIABLE', 'UNIDAD']
            columnas_existentes = [col for col in orden_columnas if col in df.columns]
            df = df[columnas_existentes]
            
            # Guardar CSV
            df.to_csv(ruta_csv, index=False, encoding='utf-8')
            
            # Calcular estadísticas
            total_registros = len(df)
            valores_validos = df['VALOR'].count()
            porcentaje_validos = (valores_validos / total_registros * 100) if total_registros > 0 else 0
            
            print(f"💾 CSV guardado: {nombre_archivo}")
            print(f"📊 Resumen del archivo:")
            print(f"   📈 Total registros: {total_registros}")
            print(f"   ✅ Valores no nulos: {valores_validos} ({porcentaje_validos:.1f}%)")
            print(f"   📅 Período: {df['FECHA'].min()} a {df['FECHA'].max()}")
            
            # Mostrar variables guardadas
            variables_guardadas = df['VARIABLE'].unique()
            print(f"   🔧 Variables: {len(variables_guardadas)}")
            for var in variables_guardadas:
                print(f"      - {var}")
            
            return ruta_csv
            
        except Exception as e:
            print(f"❌ Error guardando CSV: {e}")
            return None

def descargar_cuenca_completa(codigo_cuenca, nombre_cuenca, categorias=["Climática", "Hidrométrica"]):
    """Descargar cuenca completa - SOLO CSV"""
    downloader = SNIRHDownloader()
    
    print(f"🌊 INICIANDO DESCARGA DE CUENCA: {nombre_cuenca}")
    print(f"🔢 Código: {codigo_cuenca}")
    print(f"📂 Categorías: {', '.join(categorias)}")
    print(f"💾 Formato: Solo CSV")
    print("=" * 60)
    
    archivos_descargados = []
    
    for categoria in categorias:
        print(f"\n🎯 PROCESANDO CATEGORÍA: {categoria}")
        print("-" * 40)
        
        # Obtener estaciones
        estaciones = downloader.get_stations_by_basin(codigo_cuenca, categoria)
        
        if not estaciones:
            print(f"❌ No se encontraron estaciones {categoria}")
            continue
        
        print(f"📊 Estaciones encontradas: {len(estaciones)}")
        
        # Mostrar estaciones
        for i, estacion in enumerate(estaciones[:5]):
            nombre_estacion = downloader.clean_name(estacion.get('NOM', ''))
            print(f"   {i+1}. {nombre_estacion} (ID: {estacion.get('ID')})")
        
        # Descargar primeras 3 estaciones
        for i, estacion in enumerate(estaciones): # estaciones[:3]
            id_estacion = estacion.get('ID', '')
            nombre_estacion = downloader.clean_name(estacion.get('NOM', ''))
            
            print(f"\n🚀 [{i+1}/{len(estaciones[:3])}] Descargando: {nombre_estacion}")
            print(f"   🆔 ID: {id_estacion}")
            print(f"   📂 Categoría: {categoria}")
            print(f"   🔧 Tipo: {estacion.get('TIP', '')}")
            
            try:
                # Descargar datos
                resultados = downloader.download_station_data(id_estacion, nombre_estacion, categoria)
                
                if resultados:
                    # Guardar CSV
                    ruta_csv = downloader.save_to_csv(resultados, id_estacion, nombre_estacion, categoria)
                    
                    if ruta_csv:
                        archivos_descargados.append({
                            'estacion': nombre_estacion,
                            'categoria': categoria,
                            'archivo': ruta_csv
                        })
                        print(f"   ✅ DESCARGA EXITOSA")
                    else:
                        print(f"   ❌ Error guardando archivo")
                else:
                    print(f"   ❌ No se obtuvieron datos")
                    
            except Exception as e:
                print(f"   💥 Error: {e}")
                import traceback
                print(f"   🔍 Detalles: {traceback.format_exc()}")
            
            # Pausa entre estaciones
            if i < len(estaciones[:3]) - 1:
                print("   ⏳ Pausa entre estaciones...")
                time.sleep(5)
    
    # Resumen final
    print(f"\n{'='*60}")
    print("🎉 DESCARGA COMPLETADA")
    print(f"📁 Carpeta: {downloader.download_folder}")
    
    if archivos_descargados:
        print(f"📊 Archivos CSV descargados: {len(archivos_descargados)}")
        
        # Resumen por categoría
        for categoria in categorias:
            archivos_cat = [a for a in archivos_descargados if a['categoria'] == categoria]
            if archivos_cat:
                print(f"\n📂 {categoria}: {len(archivos_cat)} estaciones")
                for archivo in archivos_cat:
                    nombre_archivo = os.path.basename(archivo['archivo'])
                    print(f"   ✅ {archivo['estacion']}")
                    print(f"      📄 {nombre_archivo}")
    else:
        print("❌ No se descargaron archivos")
    
    return archivos_descargados

def explorar_cuencas():
    """Explorar cuencas disponibles"""
    downloader = SNIRHDownloader()
    
    print("🗺️ EXPLORANDO CUENCAS DISPONIBLES")
    print("=" * 50)
    
    cuencas = downloader.get_basins()
    
    if not cuencas:
        print("❌ No se pudieron obtener cuencas")
        return
    
    print(f"📋 Total de cuencas: {len(cuencas)}")
    
    # Mostrar primeras 10 cuencas con estadísticas
    for i, cuenca in enumerate(cuencas[:10]):
        codigo = cuenca.get('CodigoUH', '')
        nombre = cuenca.get('NombreUH', '')
        
        print(f"\n{i+1}. {nombre}")
        print(f"   Código: {codigo}")
        
        # Contar estaciones por categoría
        for categoria in ["Climática", "Hidrométrica"]:
            estaciones = downloader.get_stations_by_basin(codigo, categoria)
            print(f"   📈 {categoria}: {len(estaciones)} estaciones")

# PROGRAMA PRINCIPAL
if __name__ == "__main__":
    print("📥 DESCARGADOR SNIRH - SOLO CSV")
    print("📍 Descarga datos Climáticos e Hidrométricos")
    print("💾 Formato: CSV con fechas limpias")
    print("=" * 60)
    
    # Opción 1: Explorar cuencas primero
    explorar_cuencas()
    
    # Opción 2: Descargar cuenca específica (DESCOMENTAR PARA USAR)
    # descargar_cuenca_completa("019", "Cuenca Azangaro", ["Climática", "Hidrométrica"])
    
    # Opción 3: Probar con una estación específica (DESCOMENTAR PARA USAR)
    # downloader = SNIRHDownloader()
    # resultados = downloader.download_station_data("37", "Ananea 000826", "Climática")
    # if resultados:
    #     downloader.save_to_csv(resultados, "37", "Ananea 000826", "Climática")
    # Opción 2: Descargar cuenca específica
    # descargar_cuenca_completa_simple("019", "Cuenca Azangaro", ["Climática", "Hidrométrica"])
    
    # Opción 3: Descargar estación específica
    # downloader = SNIRHDownloaderSimple()
    # resultados = downloader.download_station_data("37", "Ananea 000826", "Climática")
    # if resultados:
    #     downloader.save_to_csv_only(resultados, "37", "Ananea 000826", "Climática")
# Descargar cuenca completa
descargar_cuenca_completa("019", "Cuenca Azangaro", ["Climática", "Hidrométrica"])

# O explorar primero



import requests
import json
import re
import pandas as pd
from datetime import datetime
import time
import os

class SNIRHDownloader:
    def __init__(self, download_folder="D:\\G"):
        self.session = requests.Session()
        self.download_folder = download_folder
        self.setup_headers()
        
        # Create folder if it doesn't exist
        os.makedirs(self.download_folder, exist_ok=True)
        print(f"📁 Download folder: {self.download_folder}")
        
    def setup_headers(self):
        """Configure headers for requests"""
        self.session.headers.update({
            'Content-Type': 'application/json; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://snirh.ana.gob.pe/ANDREA/Integrado.aspx',
            'Origin': 'https://snirh.ana.gob.pe',
            'X-Requested-With': 'XMLHttpRequest'
        })
    
    def clean_name(self, text):
        """Clean station/variable name"""
        if not text:
            return ""
        # Clean HTML
        clean_text = re.sub(r'<[^>]+>', '', text)
        # Clean spaces
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        # Clean for filename
        clean_text = re.sub(r'[\\/*?:"<>|]', '', clean_text)
        return clean_text
    
    def get_basins(self):
        """Get list of available basins"""
        url = "https://snirh.ana.gob.pe/ANDREA/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": "", "pBuscar": "", "pTipo": "UH"}
        
        try:
            print("📋 Getting basin list...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                basins = json.loads(response.json()['d'])
                print(f"✅ Basins obtained: {len(basins)}")
                return basins
            else:
                print(f"❌ Error getting basins: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception getting basins: {e}")
            return []
    
    def get_stations_by_basin(self, basin_code, category=None):
        """Get stations from a specific basin"""
        url = "https://snirh.ana.gob.pe/ANDREA/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": basin_code, "pBuscar": "", "pTipo": "EST"}
        
        try:
            print(f"📡 Searching stations for basin {basin_code}...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                stations = json.loads(response.json()['d'])
                
                # Filter by category if specified
                if category:
                    stations = [s for s in stations if s.get('CAT') == category]
                    print(f"✅ {category} stations: {len(stations)}")
                
                print(f"✅ Total stations: {len(stations)}")
                return stations
            else:
                print(f"❌ Error getting stations: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception getting stations: {e}")
            return []
    
    def get_station_variables(self, station_id):
        """Get available variables for a station"""
        url = "https://snirh.ana.gob.pe/ANDREA/index.aspx/VariableListarxEstacion"
        data = {
            "pIDEstacion": station_id,
            "pTipo": "DMA"  # Daily, Monthly, Annual
        }
        
        try:
            print(f"🔧 Getting variables for station {station_id}...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result:
                    variables = json.loads(result['d'])
                    print(f"✅ Variables found: {len(variables)}")
                    return variables
                else:
                    print("⚠️ No variables found in response")
                    return []
            else:
                print(f"❌ Error getting variables: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception getting variables: {e}")
            return []
    
    def get_registry_types(self, station_id, operator_id, variable_id, study_id):
        """Get available registry types"""
        url = "https://snirh.ana.gob.pe/ANDREA/Index.aspx/TipoRegistroListar"
        data = {
            "pIDEstacion": station_id,
            "pIDOperador": operator_id,
            "pIDVariable": variable_id,
            "pIDEstudio": study_id,
            "pTipoSerie": "MENSUAL",
            "pIDUsuario": 0
        }
        
        try:
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result:
                    types = json.loads(result['d'])
                    return types
                else:
                    print("⚠️ No registry types found, using default")
                    return [{"ID": 1, "NOMBRE": "Default"}]
            else:
                print(f"⚠️ Error getting registry types, using default: {response.status_code}")
                return [{"ID": 1, "NOMBRE": "Default"}]
        except Exception as e:
            print(f"⚠️ Exception getting registry types, using default: {e}")
            return [{"ID": 1, "NOMBRE": "Default"}]
    
    def download_data_series(self, station_id, operator_id, variable_id, study_id, registry_type_id=1):
        """Download complete data series"""
        url = "https://snirh.ana.gob.pe/ANDREA/ServicioGeneral.asmx/SerieDatos"
        data = {
            "IDEstacion": station_id,
            "IDOperador": operator_id,
            "IDVariable": variable_id,
            "TipoSerie": "MENSUAL",
            "pIDEstudio": study_id,
            "IDUsuario": 0,
            "IDTipoRegistro": registry_type_id
        }
        
        try:
            print(f"📥 Downloading data...")
            response = self.session.post(url, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if 'd' in result and result['d']:
                    data_series = result['d']
                    print(f"✅ Data downloaded: {len(data_series)} records")
                    return data_series
                else:
                    print("❌ No data found in response")
                    print(f"📄 Complete response: {result}")
                    return []
            else:
                print(f"❌ Error downloading series: {response.status_code}")
                print(f"📄 Response: {response.text}")
                return []
        except Exception as e:
            print(f"❌ Exception downloading series: {e}")
            return []
    
    def process_monthly_data(self, data_series, variable_name, unit_measure):
        """Process monthly data to tabular format"""
        processed_data = []
        
        for item in data_series:
            if isinstance(item, dict):
                # Format: {"Dato01": year, "Dato02": january, ..., "Dato14": total}
                row = {
                    'YEAR': item.get('Dato01', ''),
                    'JANUARY': self.convert_number(item.get('Dato02')),
                    'FEBRUARY': self.convert_number(item.get('Dato03')),
                    'MARCH': self.convert_number(item.get('Dato04')),
                    'APRIL': self.convert_number(item.get('Dato05')),
                    'MAY': self.convert_number(item.get('Dato06')),
                    'JUNE': self.convert_number(item.get('Dato07')),
                    'JULY': self.convert_number(item.get('Dato08')),
                    'AUGUST': self.convert_number(item.get('Dato09')),
                    'SEPTEMBER': self.convert_number(item.get('Dato10')),
                    'OCTOBER': self.convert_number(item.get('Dato11')),
                    'NOVEMBER': self.convert_number(item.get('Dato12')),
                    'DECEMBER': self.convert_number(item.get('Dato13')),
                    'TOTAL_AVERAGE': self.convert_number(item.get('Dato14'))
                }
                # Only add if it has valid year
                if row['YEAR'] and str(row['YEAR']).isdigit():
                    processed_data.append(row)
        
        return processed_data
    
    def convert_number(self, value):
        """Convert value to number if possible"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return value
    
    def download_daily_data(self, station_id, variable_info):
        """Download daily data in [date, value] format"""
        url = "https://snirh.ana.gob.pe/ANDREA/ServicioGeneral.asmx/SerieDatos"
        data = {
            "IDEstacion": station_id,
            "IDOperador": variable_info.get('IDOPERADOR', ''),
            "IDVariable": variable_info.get('ID', ''),
            "TipoSerie": "DIARIA",
            "pIDEstudio": variable_info.get('IDESTUDIO', ''),
            "IDUsuario": 0,
            "IDTipoRegistro": 1
        }
        
        try:
            print(f"📥 Downloading daily data...")
            response = self.session.post(url, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if 'd' in result and result['d']:
                    data_series = result['d']
                    print(f"✅ Daily data downloaded: {len(data_series)} records")
                    return data_series
                else:
                    print("❌ No daily data found in response")
                    return []
            else:
                print(f"❌ Error downloading daily series: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception downloading daily series: {e}")
            return []
    
    def process_daily_data(self, data_series):
        """Process data in list format [date, value]"""
        if not data_series:
            return []
        
        processed_data = []
        
        for item in data_series:
            if isinstance(item, list) and len(item) >= 2:
                date = item[0]
                value = item[1]
                
                # Process date
                date_dt = self._process_date(date)
                
                row = {
                    'DATE': date_dt,
                    'YEAR': date_dt.year if isinstance(date_dt, datetime) else None,
                    'MONTH': date_dt.month if isinstance(date_dt, datetime) else None,
                    'DAY': date_dt.day if isinstance(date_dt, datetime) else None,
                    'VALUE': self._convert_number_daily(value)
                }
                processed_data.append(row)
        
        return processed_data
    
    def _process_date(self, date):
        """Convert date to datetime"""
        if isinstance(date, str):
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
            for fmt in formats:
                try:
                    return datetime.strptime(date.strip(), fmt)
                except ValueError:
                    continue
        return date
    
    def _convert_number_daily(self, value):
        """Convert value to number for daily data"""
        if value is None or value == '':
            return None
        try:
            if isinstance(value, str):
                value = value.strip().replace(',', '.')
                if value in ['', 'NA', 'N/A']:
                    return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def download_station_complete_data(self, station_id, station_name="", category=""):
        """Download all available data for a station"""
        print(f"\n🎯 PROCESSING STATION: {station_id} - {station_name}")
        print("-" * 50)
        
        # 1. Get station variables
        variables = self.get_station_variables(station_id)
        if not variables:
            print("❌ No variables found for this station")
            return None
        
        results = {}
        
        # Process each variable
        for i, variable in enumerate(variables):
            var_name = self.clean_name(variable.get('NOMBRE', ''))
            print(f"\n📋 Variable {i+1}/{len(variables)}: {var_name}")
            
            # Filter only daily data variables (to avoid errors)
            if "1DIA" in var_name.upper() or "DIAR" in var_name.upper():
                print(f"   Processing daily data...")
                
                # Download daily data
                data_series = self.download_daily_data(station_id, variable)
                
                if data_series:
                    # Process data
                    processed_data = self.process_daily_data(data_series)
                    
                    if processed_data:
                        key = f"{var_name}_{variable.get('UNIDADMEDIDA', '')}"
                        key = key.replace(' ', '_')[:50]
                        
                        # Calculate period
                        if processed_data:
                            dates = [d['DATE'] for d in processed_data if d['DATE']]
                            if dates:
                                min_date = min(dates)
                                max_date = max(dates)
                                period = f"{min_date} - {max_date}"
                            else:
                                period = "N/A"
                        else:
                            period = "N/A"
                        
                        results[key] = {
                            'data': processed_data,
                            'variable_info': variable,
                            'statistics': {
                                'total_records': len(processed_data),
                                'period': period,
                                'unit_measure': variable.get('UNIDADMEDIDA', '')
                            }
                        }
                        print(f"✅ Data processed: {len(processed_data)} records ({period})")
                    else:
                        print("⚠️ Could not process data")
                else:
                    print("❌ No data obtained for this variable")
            else:
                print(f"   ⏭️  Skipping (daily data only)")
            
            # Pause to not overload server
            time.sleep(1)
        
        return results if results else None
    
    def save_results(self, results, station_id, station_name, category=""):
        """Save results in Excel and JSON - WITH CORRECT COLUMN ORDER"""
        if not results:
            print("❌ No results to save")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_name = self.clean_name(station_name)
        clean_name = clean_name.replace(' ', '_')[:50]
        
        # Include category in name
        category_suffix = f"_{category}" if category else ""
        
        # Excel file
        excel_name = f"SNIRH_{station_id}_{clean_name}{category_suffix}_{timestamp}.xlsx"
        excel_path = os.path.join(self.download_folder, excel_name)
        
        try:
            # Save Excel with multiple sheets
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for variable_name, data in results.items():
                    df = pd.DataFrame(data['data'])
                    
                    # ============================================================
                    # CORRECT COLUMN ORDER: DATE, YEAR, MONTH, DAY, VALUE
                    # ============================================================
                    desired_order = ['DATE', 'YEAR', 'MONTH', 'DAY', 'VALUE']
                    
                    # Check which columns actually exist
                    existing_columns = [col for col in desired_order if col in df.columns]
                    
                    # Reorder columns
                    df = df[existing_columns]
                    # ============================================================
                    
                    # Clean sheet name
                    sheet_name = re.sub(r'[\\/*?:\[\]]', '', variable_name)[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"💾 Excel saved: {excel_path}")
            
            # JSON file with metadata
            json_name = f"SNIRH_{station_id}_{clean_name}{category_suffix}_{timestamp}_metadata.json"
            json_path = os.path.join(self.download_folder, json_name)
            
            metadata = {
                'station_id': station_id,
                'station_name': station_name,
                'category': category,
                'download_date': datetime.now().isoformat(),
                'total_variables': len(results),
                'variables': {
                    var_name: {
                        'statistics': data['statistics'],
                        'info': data['variable_info']
                    } for var_name, data in results.items()
                }
            }
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            print(f"📋 Metadata saved: {json_path}")
            
            return {
                'excel': excel_path,
                'json': json_path
            }
            
        except Exception as e:
            print(f"❌ Error saving files: {e}")
            return None

def download_complete_basin(basin_code, basin_name, categories=["Climática", "Hidrométrica"]):
    """Download all stations from a complete basin"""
    downloader = SNIRHDownloader()
    
    print(f"🌊 STARTING BASIN DOWNLOAD: {basin_name}")
    print(f"📊 Categories: {', '.join(categories)}")
    print("=" * 60)
    
    downloaded_files = []
    
    for category in categories:
        print(f"\n🎯 PROCESSING CATEGORY: {category}")
        print("-" * 40)
        
        # Get stations from the basin - USING CORRECT NAMES
        stations = downloader.get_stations_by_basin(basin_code, category)
        
        if not stations:
            print(f"❌ No {category} stations found")
            continue
        
        print(f"📊 Stations found: {len(stations)}")
        
        # Download each station (limit to 3 for testing)
        for i, station in enumerate(stations[:3]):
            station_id = station.get('ID', '')
            station_name_html = station.get('NOM', '')
            clean_name = downloader.clean_name(station_name_html)
            
            print(f"\n🚀 Station {i+1}/{min(3, len(stations))}: {clean_name}")
            print(f"   🆔 ID: {station_id}")
            print(f"   📂 Category: {station.get('CAT', '')}")
            print(f"   🔧 Type: {station.get('TIP', '')}")
            
            try:
                # Download data
                results = downloader.download_station_complete_data(
                    station_id, 
                    clean_name, 
                    category
                )
                
                if results:
                    # Save results
                    paths = downloader.save_results(
                        results, 
                        station_id, 
                        clean_name, 
                        category
                    )
                    
                    if paths:
                        downloaded_files.append({
                            'station': clean_name,
                            'category': category,
                            'files': paths
                        })
                    
                    # Summary
                    print("📊 Data summary:")
                    for var_name, data_var in results.items():
                        stats = data_var['statistics']
                        print(f"   - {var_name}: {stats['total_records']} years ({stats['period']})")
                else:
                    print("❌ No data obtained")
                    
            except Exception as e:
                print(f"❌ Error processing station: {e}")
                continue
            
            # Pause between stations
            if i < min(3, len(stations)) - 1:
                print("⏳ Pause...")
                time.sleep(3)
    
    # Final summary
    print(f"\n{'='*60}")
    print("🎉 DOWNLOAD COMPLETED")
    print(f"📁 Folder: {downloader.download_folder}")
    
    if downloaded_files:
        print(f"📊 Total files downloaded: {len(downloaded_files)}")
        for file in downloaded_files:
            print(f"   - {file['station']} ({file['category']})")
    else:
        print("❌ No files downloaded")

def explore_basins():
    """Explore available basins"""
    downloader = SNIRHDownloader()
    
    print("🌊 EXPLORING AVAILABLE BASINS")
    print("=" * 50)
    
    basins = downloader.get_basins()
    
    if not basins:
        print("❌ Could not get basins")
        return
    
    print(f"\n📋 TOTAL BASINS: {len(basins)}")
    print("\n📊 FIRST 10 BASINS:")
    
    for i, basin in enumerate(basins[:10]):
        code = basin.get('CodigoUH', '')
        name = basin.get('NombreUH', '')
        
        print(f"\n{i+1}. {name}")
        print(f"   Code: {code}")
        
        # Count stations by category - USING CORRECT NAMES
        for category in ["Climática", "Hidrométrica"]:
            stations = downloader.get_stations_by_basin(code, category)
            print(f"   📈 {category}: {len(stations)} stations")
            
            # Show first 2 stations if they exist
            if stations:
                for j, station in enumerate(stations[:2]):
                    station_name = downloader.clean_name(station.get('NOM', ''))
                    print(f"      {j+1}. {station_name} (ID: {station.get('ID')})")

def download_specific_station(station_id, station_name="", category=""):
    """Download a specific station"""
    downloader = SNIRHDownloader()
    
    print(f"🎯 DOWNLOADING SPECIFIC STATION")
    print(f"🏷️  ID: {station_id}")
    if station_name:
        print(f"📝 Name: {station_name}")
    if category:
        print(f"📂 Category: {category}")
    print("-" * 40)
    
    results = downloader.download_station_complete_data(station_id, station_name, category)
    
    if results:
        paths = downloader.save_results(results, station_id, station_name, category)
        
        if paths:
            print(f"\n🎉 SUCCESSFUL DOWNLOAD")
            print(f"📄 Excel: {os.path.basename(paths['excel'])}")
            print(f"📋 JSON: {os.path.basename(paths['json'])}")
            
            print("\n📊 SUMMARY:")
            for var_name, data_var in results.items():
                stats = data_var['statistics']
                print(f"   - {var_name}: {stats['total_records']} years ({stats['period']})")
    else:
        print("❌ Could not get data")

# SPECIAL FUNCTION TO TEST WITH STATIONS WE KNOW HAVE DATA
def test_known_stations():
    """Test with stations we know exist from previous exploration"""
    downloader = SNIRHDownloader()
    
    print("🧪 TESTING KNOWN STATIONS")
    print("=" * 50)
    
    # Stations from previous exploration that have variables
    test_stations = [
        {"id": "37", "name": "Ananea 000826", "category": "Climática", "basin": "Azangaro"},
        {"id": "55", "name": "Antauta 157406", "category": "Climática", "basin": "Azangaro"},
        {"id": "94", "name": "Azangaro 000781", "category": "Climática", "basin": "Azangaro"},
        {"id": "437", "name": "Cuzco 000389", "category": "Climática", "basin": "Biabo"},
        {"id": "199", "name": "Cañaveral 000136", "category": "Climática", "basin": "Bocapán"}
    ]
    
    for station in test_stations:
        print(f"\n🔍 Testing: {station['name']} (ID: {station['id']})")
        
        # Only get variables to verify
        variables = downloader.get_station_variables(station['id'])
        
        if variables:
            print(f"✅ HAS {len(variables)} VARIABLES")
            for var in variables[:2]:  # Show first 2
                print(f"   - {downloader.clean_name(var.get('NOMBRE', ''))}")
            
            # Ask if download
            response = input("   Download data? (y/n): ").lower()
            if response == 'y':
                download_specific_station(
                    station['id'], 
                    station['name'], 
                    station['category']
                )
        else:
            print("❌ No variables")
        
        time.sleep(2)

if __name__ == "__main__":
    print("📥 SNIRH-ANA DOWNLOADER (CORRECTED)")
    print("📍 Categories: 'Climática' and 'Hidrométrica'")
    print("=" * 50)
    
    # Option 1: Explore basins first (RECOMMENDED)
    explore_basins()
    
    # Option 2: Test known stations
    # test_known_stations()
    
    # Option 3: Download specific basin
    # download_complete_basin("019", "Cuenca Azangaro", ["Climática"])
    
    # Option 4: Download specific station
download_specific_station("13718", "Cuenca Acarí", "Climática")

# Esto descargará estaciones CLIMÁTICAS E HIDROMÉTRICAS de la cuenca
download_complete_basin("019", "Cuenca Acarí", ["Climática", "Hidrométrica"])



####################################################################################
#       CLIMATICAS Y HIDROMETRICAS
##################################################################################
import requests
import json
import re
import pandas as pd
from datetime import datetime
import time
import os

class SNIRHDownloader:
    def __init__(self, carpeta_descargas="D:\\G"):
        self.session = requests.Session()
        self.carpeta_descargas = carpeta_descargas
        self.setup_headers()
        
        # Crear carpeta si no existe
        os.makedirs(self.carpeta_descargas, exist_ok=True)
        print(f"📁 Carpeta de descargas: {self.carpeta_descargas}")
        
    def setup_headers(self):
        """Configurar headers para las peticiones"""
        self.session.headers.update({
            'Content-Type': 'application/json; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://snirh.ana.gob.pe/ANDREA/Integrado.aspx',
            'Origin': 'https://snirh.ana.gob.pe',
            'X-Requested-With': 'XMLHttpRequest'
        })
    
    def obtener_cuencas(self):
        """Obtener lista de cuencas disponibles"""
        url = "https://snirh.ana.gob.pe/ANDREA/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": "", "pBuscar": "", "pTipo": "UH"}
        
        try:
            print("📋 Obteniendo lista de cuencas...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                cuencas = json.loads(response.json()['d'])
                print(f"✅ Cuencas obtenidas: {len(cuencas)}")
                return cuencas
            else:
                print(f"❌ Error al obtener cuencas: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Excepción al obtener cuencas: {e}")
            return []
    
    def obtener_estaciones_por_cuenca(self, codigo_cuenca, categoria=None):
        """Obtener estaciones de una cuenca específica - CORREGIDO"""
        url = "https://snirh.ana.gob.pe/ANDREA/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": codigo_cuenca, "pBuscar": "", "pTipo": "EST"}
        
        try:
            print(f"📡 Buscando estaciones para cuenca {codigo_cuenca}...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                estaciones = json.loads(response.json()['d'])
                
                # Filtrar por categoría si se especifica - CORREGIDO
                if categoria:
                    # Las categorías son "Climática" e "Hidrométrica" (en español)
                    estaciones_filtradas = [e for e in estaciones if e.get('CAT') == categoria]
                    print(f"✅ Estaciones {categoria}: {len(estaciones_filtradas)}")
                    return estaciones_filtradas
                
                print(f"✅ Estaciones totales: {len(estaciones)}")
                return estaciones
            else:
                print(f"❌ Error al obtener estaciones: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Excepción al obtener estaciones: {e}")
            return []
    
    def obtener_variables_estacion(self, id_estacion):
        """Obtener variables disponibles para una estación"""
        url = "https://snirh.ana.gob.pe/ANDREA/index.aspx/VariableListarxEstacion"
        data = {
            "pIDEstacion": id_estacion,
            "pTipo": "DMA"  # Diario, Mensual, Anual
        }
        
        try:
            print(f"🔧 Obteniendo variables para estación {id_estacion}...")
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                resultado = response.json()
                if 'd' in resultado:
                    variables = json.loads(resultado['d'])
                    print(f"✅ Variables encontradas: {len(variables)}")
                    return variables
                else:
                    print("⚠️ No se encontraron variables en la respuesta")
                    return []
            else:
                print(f"❌ Error al obtener variables: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Excepción al obtener variables: {e}")
            return []
    
    def obtener_tipos_registro(self, id_estacion, id_operador, id_variable, id_estudio):
        """Obtener tipos de registro disponibles"""
        url = "https://snirh.ana.gob.pe/ANDREA/Index.aspx/TipoRegistroListar"
        data = {
            "pIDEstacion": id_estacion,
            "pIDOperador": id_operador,
            "pIDVariable": id_variable,
            "pIDEstudio": id_estudio,
            "pTipoSerie": "MENSUAL",
            "pIDUsuario": 0
        }
        
        try:
            response = self.session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                resultado = response.json()
                if 'd' in resultado:
                    tipos = json.loads(resultado['d'])
                    return tipos
                else:
                    print("⚠️ No se encontraron tipos de registro, usando valor por defecto")
                    return [{"ID": 1, "NOMBRE": "Por defecto"}]
            else:
                print(f"⚠️ Error al obtener tipos de registro, usando valor por defecto: {response.status_code}")
                return [{"ID": 1, "NOMBRE": "Por defecto"}]
        except Exception as e:
            print(f"⚠️ Excepción al obtener tipos de registro, usando valor por defecto: {e}")
            return [{"ID": 1, "NOMBRE": "Por defecto"}]
    
    def descargar_serie_datos(self, id_estacion, id_operador, id_variable, id_estudio, id_tipo_registro=1):
        """Descargar serie de datos completa - CORREGIDO CON MEJOR MANEJO DE ERRORES"""
        url = "https://snirh.ana.gob.pe/ANDREA/ServicioGeneral.asmx/SerieDatos"
        data = {
            "IDEstacion": id_estacion,
            "IDOperador": id_operador,
            "IDVariable": id_variable,
            "TipoSerie": "MENSUAL",
            "pIDEstudio": id_estudio,
            "IDUsuario": 0,
            "IDTipoRegistro": id_tipo_registro
        }
        
        try:
            print(f"📥 Descargando datos...")
            response = self.session.post(url, json=data, timeout=60)
            
            if response.status_code == 200:
                resultado = response.json()
                if 'd' in resultado and resultado['d']:
                    serie_datos = resultado['d']
                    print(f"✅ Datos descargados: {len(serie_datos)} registros")
                    return serie_datos
                else:
                    print("❌ No se encontraron datos en la respuesta")
                    print(f"📄 Respuesta completa: {resultado}")
                    return []
            else:
                print(f"❌ Error al descargar serie: {response.status_code}")
                print(f"📄 Respuesta: {response.text}")
                return []
        except Exception as e:
            print(f"❌ Excepción al descargar serie: {e}")
            return []
    
    def procesar_datos_mensuales(self, serie_datos, nombre_variable, unidad_medida):
        """Procesar datos mensuales a formato tabular"""
        datos_procesados = []
        
        for item in serie_datos:
            if isinstance(item, dict):
                # Formato: {"Dato01": año, "Dato02": enero, ..., "Dato14": total}
                fila = {
                    'AÑO': item.get('Dato01', ''),
                    'ENERO': self.convertir_numero(item.get('Dato02')),
                    'FEBRERO': self.convertir_numero(item.get('Dato03')),
                    'MARZO': self.convertir_numero(item.get('Dato04')),
                    'ABRIL': self.convertir_numero(item.get('Dato05')),
                    'MAYO': self.convertir_numero(item.get('Dato06')),
                    'JUNIO': self.convertir_numero(item.get('Dato07')),
                    'JULIO': self.convertir_numero(item.get('Dato08')),
                    'AGOSTO': self.convertir_numero(item.get('Dato09')),
                    'SEPTIEMBRE': self.convertir_numero(item.get('Dato10')),
                    'OCTUBRE': self.convertir_numero(item.get('Dato11')),
                    'NOVIEMBRE': self.convertir_numero(item.get('Dato12')),
                    'DICIEMBRE': self.convertir_numero(item.get('Dato13')),
                    'TOTAL_PROMEDIO': self.convertir_numero(item.get('Dato14'))
                }
                # Solo agregar si tiene año válido
                if fila['AÑO'] and str(fila['AÑO']).isdigit():
                    datos_procesados.append(fila)
        
        return datos_procesados
    
    def convertir_numero(self, valor):
        """Convertir valor a número si es posible"""
        if valor is None or valor == '':
            return None
        try:
            return float(valor)
        except (ValueError, TypeError):
            return valor
    
    def limpiar_nombre(self, texto):
        """Limpiar nombre de estación/variable"""
        if not texto:
            return ""
        # Limpiar HTML
        texto_limpio = re.sub(r'<[^>]+>', '', texto)
        # Limpiar espacios
        texto_limpio = re.sub(r'\s+', ' ', texto_limpio).strip()
        # Limpiar para nombre de archivo
        texto_limpio = re.sub(r'[\\/*?:"<>|]', '', texto_limpio)
        return texto_limpio
    
    def descargar_datos_estacion_completa(self, id_estacion, nombre_estacion="", categoria=""):
        """Descargar todos los datos disponibles para una estación - MEJORADO"""
        print(f"\n🎯 PROCESANDO ESTACIÓN: {id_estacion} - {nombre_estacion}")
        print("-" * 50)
        
        # 1. Obtener variables de la estación
        variables = self.obtener_variables_estacion(id_estacion)
        if not variables:
            print("❌ No se encontraron variables para esta estación")
            return None
        
        resultados = {}
        
        # Procesar cada variable
        for i, variable in enumerate(variables):
            nombre_var = self.limpiar_nombre(variable.get('NOMBRE', ''))
            print(f"\n📋 Variable {i+1}/{len(variables)}: {nombre_var}")
            
            # 2. Obtener tipos de registro
            tipos_registro = self.obtener_tipos_registro(
                id_estacion,
                variable.get('IDOPERADOR'),
                variable.get('ID'),
                variable.get('IDESTUDIO')
            )
            
            # Usar el primer tipo de registro disponible
            id_tipo_registro = tipos_registro[0].get('ID', 1) if tipos_registro else 1
            
            # 3. Descargar serie de datos
            serie_datos = self.descargar_serie_datos(
                id_estacion,
                variable.get('IDOPERADOR'),
                variable.get('ID'),
                variable.get('IDESTUDIO'),
                id_tipo_registro
            )
            
            if serie_datos:
                # 4. Procesar datos
                datos_procesados = self.procesar_datos_mensuales(
                    serie_datos,
                    nombre_var,
                    variable.get('UNIDADMEDIDA', '')
                )
                
                if datos_procesados:
                    # Crear clave única para la variable
                    clave = f"{nombre_var}_{variable.get('UNIDADMEDIDA', '')}"
                    clave = clave.replace(' ', '_')[:50]
                    
                    # Calcular período
                    años = [d['AÑO'] for d in datos_procesados if d['AÑO']]
                    periodo = f"{min(años)}-{max(años)}" if años else "N/A"
                    
                    resultados[clave] = {
                        'datos': datos_procesados,
                        'variable_info': variable,
                        'estadisticas': {
                            'total_registros': len(datos_procesados),
                            'periodo': periodo,
                            'unidad_medida': variable.get('UNIDADMEDIDA', '')
                        }
                    }
                    print(f"✅ Datos procesados: {len(datos_procesados)} años ({periodo})")
                else:
                    print("⚠️ No se pudieron procesar los datos")
            else:
                print("❌ No se obtuvieron datos para esta variable")
            
            # Pausa para no saturar el servidor
            time.sleep(1)
        
        return resultados if resultados else None
    
    def guardar_resultados(self, resultados, id_estacion, nombre_estacion, categoria=""):
        """Guardar resultados en Excel y JSON"""
        if not resultados:
            print("❌ No hay resultados para guardar")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_limpio = self.limpiar_nombre(nombre_estacion)
        nombre_limpio = nombre_limpio.replace(' ', '_')[:50]
        
        # Incluir categoría en el nombre
        categoria_sufijo = f"_{categoria}" if categoria else ""
        
        # Archivo Excel
        nombre_excel = f"SNIRH_{id_estacion}_{nombre_limpio}{categoria_sufijo}_{timestamp}.xlsx"
        ruta_excel = os.path.join(self.carpeta_descargas, nombre_excel)
        
        try:
            # Guardar Excel con múltiples hojas
            with pd.ExcelWriter(ruta_excel, engine='openpyxl') as writer:
                for nombre_variable, datos in resultados.items():
                    df = pd.DataFrame(datos['datos'])
                    # Limpiar nombre de hoja
                    nombre_hoja = re.sub(r'[\\/*?:\[\]]', '', nombre_variable)[:31]
                    df.to_excel(writer, sheet_name=nombre_hoja, index=False)
            
            print(f"💾 Excel guardado: {ruta_excel}")
            
            # Archivo JSON con metadatos
            nombre_json = f"SNIRH_{id_estacion}_{nombre_limpio}{categoria_sufijo}_{timestamp}_metadatos.json"
            ruta_json = os.path.join(self.carpeta_descargas, nombre_json)
            
            metadatos = {
                'estacion_id': id_estacion,
                'estacion_nombre': nombre_estacion,
                'categoria': categoria,
                'fecha_descarga': datetime.now().isoformat(),
                'total_variables': len(resultados),
                'variables': {
                    nombre_var: {
                        'estadisticas': datos['estadisticas'],
                        'info': datos['variable_info']
                    } for nombre_var, datos in resultados.items()
                }
            }
            
            with open(ruta_json, 'w', encoding='utf-8') as f:
                json.dump(metadatos, f, ensure_ascii=False, indent=2)
            
            print(f"📋 Metadatos guardados: {ruta_json}")
            
            return {
                'excel': ruta_excel,
                'json': ruta_json
            }
            
        except Exception as e:
            print(f"❌ Error al guardar archivos: {e}")
            return None

def descargar_cuenca_completa(codigo_cuenca, nombre_cuenca, categorias=["Climática", "Hidrométrica"]):
    """Descargar todas las estaciones de una cuenca completa - CORREGIDO"""
    downloader = SNIRHDownloader()
    
    print(f"🌊 INICIANDO DESCARGA DE CUENCA: {nombre_cuenca}")
    print(f"📊 Categorías: {', '.join(categorias)}")
    print("=" * 60)
    
    archivos_descargados = []
    
    for categoria in categorias:
        print(f"\n🎯 PROCESANDO CATEGORÍA: {categoria}")
        print("-" * 40)
        
        # Obtener estaciones de la cuenca - USANDO NOMBRES CORRECTOS
        estaciones = downloader.obtener_estaciones_por_cuenca(codigo_cuenca, categoria)
        
        if not estaciones:
            print(f"❌ No se encontraron estaciones {categoria}")
            continue
        
        print(f"📊 Estaciones encontradas: {len(estaciones)}")
        
        # Descargar cada estación (limitar a 3 para prueba)
        for i, estacion in enumerate(estaciones[:3]):
            id_estacion = estacion.get('ID', '')
            nombre_html = estacion.get('NOM', '')
            nombre_limpio = downloader.limpiar_nombre(nombre_html)
            
            print(f"\n🚀 Estación {i+1}/{min(3, len(estaciones))}: {nombre_limpio}")
            print(f"   🆔 ID: {id_estacion}")
            print(f"   📂 Categoría: {estacion.get('CAT', '')}")
            print(f"   🔧 Tipo: {estacion.get('TIP', '')}")
            
            try:
                # Descargar datos
                resultados = downloader.descargar_datos_estacion_completa(
                    id_estacion, 
                    nombre_limpio, 
                    categoria
                )
                
                if resultados:
                    # Guardar resultados
                    rutas = downloader.guardar_resultados(
                        resultados, 
                        id_estacion, 
                        nombre_limpio, 
                        categoria
                    )
                    
                    if rutas:
                        archivos_descargados.append({
                            'estacion': nombre_limpio,
                            'categoria': categoria,
                            'archivos': rutas
                        })
                    
                    # Resumen
                    print("📊 Resumen de datos:")
                    for nombre_var, datos_var in resultados.items():
                        stats = datos_var['estadisticas']
                        print(f"   - {nombre_var}: {stats['total_registros']} años ({stats['periodo']})")
                else:
                    print("❌ No se obtuvieron datos")
                    
            except Exception as e:
                print(f"❌ Error procesando estación: {e}")
                continue
            
            # Pausa entre estaciones
            if i < min(3, len(estaciones)) - 1:
                print("⏳ Pausa...")
                time.sleep(3)
    
    # Resumen final
    print(f"\n{'='*60}")
    print("🎉 DESCARGA COMPLETADA")
    print(f"📁 Carpeta: {downloader.carpeta_descargas}")
    
    if archivos_descargados:
        print(f"📊 Total archivos descargados: {len(archivos_descargados)}")
        for archivo in archivos_descargados:
            print(f"   - {archivo['estacion']} ({archivo['categoria']})")
    else:
        print("❌ No se descargaron archivos")

def explorar_cuencas():
    """Explorar cuencas disponibles - MEJORADO"""
    downloader = SNIRHDownloader()
    
    print("🌊 EXPLORANDO CUENCAS DISPONIBLES")
    print("=" * 50)
    
    cuencas = downloader.obtener_cuencas()
    
    if not cuencas:
        print("❌ No se pudieron obtener cuencas")
        return
    
    print(f"\n📋 TOTAL CUENCAS: {len(cuencas)}")
    print("\n📊 PRIMERAS 10 CUENCAS:")
    
    for i, cuenca in enumerate(cuencas[:10]):
        codigo = cuenca.get('CodigoUH', '')
        nombre = cuenca.get('NombreUH', '')
        
        print(f"\n{i+1}. {nombre}")
        print(f"   Código: {codigo}")
        
        # Contar estaciones por categoría - USANDO NOMBRES CORRECTOS
        for categoria in ["Climática", "Hidrométrica"]:
            estaciones = downloader.obtener_estaciones_por_cuenca(codigo, categoria)
            print(f"   📈 {categoria}: {len(estaciones)} estaciones")
            
            # Mostrar primeras 2 estaciones si existen
            if estaciones:
                for j, est in enumerate(estaciones[:2]):
                    nombre_est = downloader.limpiar_nombre(est.get('NOM', ''))
                    print(f"      {j+1}. {nombre_est} (ID: {est.get('ID')})")

def descargar_estacion_especifica(id_estacion, nombre_estacion="", categoria=""):
    """Descargar una estación específica"""
    downloader = SNIRHDownloader()
    
    print(f"🎯 DESCARGANDO ESTACIÓN ESPECÍFICA")
    print(f"🏷️  ID: {id_estacion}")
    if nombre_estacion:
        print(f"📝 Nombre: {nombre_estacion}")
    if categoria:
        print(f"📂 Categoría: {categoria}")
    print("-" * 40)
    
    resultados = downloader.descargar_datos_estacion_completa(id_estacion, nombre_estacion, categoria)
    
    if resultados:
        rutas = downloader.guardar_resultados(resultados, id_estacion, nombre_estacion, categoria)
        
        if rutas:
            print(f"\n🎉 DESCARGA EXITOSA")
            print(f"📄 Excel: {os.path.basename(rutas['excel'])}")
            print(f"📋 JSON: {os.path.basename(rutas['json'])}")
            
            print("\n📊 RESUMEN:")
            for nombre_var, datos_var in resultados.items():
                stats = datos_var['estadisticas']
                print(f"   - {nombre_var}: {stats['total_registros']} años ({stats['periodo']})")
    else:
        print("❌ No se pudieron obtener datos")

# FUNCIÓN ESPECIAL PARA PROBAR CON ESTACIONES QUE SABEMOS QUE TIENEN DATOS
def prueba_estaciones_conocidas():
    """Probar con estaciones que sabemos que existen de la exploración anterior"""
    downloader = SNIRHDownloader()
    
    print("🧪 PROBANDO ESTACIONES CONOCIDAS")
    print("=" * 50)
    
    # Estaciones de la exploración anterior que tienen variables
    estaciones_prueba = [
        {"id": "37", "nombre": "Ananea 000826", "categoria": "Climática", "cuenca": "Azangaro"},
        {"id": "55", "nombre": "Antauta 157406", "categoria": "Climática", "cuenca": "Azangaro"},
        {"id": "94", "nombre": "Azangaro 000781", "categoria": "Climática", "cuenca": "Azangaro"},
        {"id": "437", "nombre": "Cuzco 000389", "categoria": "Climática", "cuenca": "Biabo"},
        {"id": "199", "nombre": "Cañaveral 000136", "categoria": "Climática", "cuenca": "Bocapán"}
    ]
    
    for estacion in estaciones_prueba:
        print(f"\n🔍 Probando: {estacion['nombre']} (ID: {estacion['id']})")
        
        # Solo obtener variables para verificar
        variables = downloader.obtener_variables_estacion(estacion['id'])
        
        if variables:
            print(f"✅ TIENE {len(variables)} VARIABLES")
            for var in variables[:2]:  # Mostrar primeras 2
                print(f"   - {downloader.limpiar_nombre(var.get('NOMBRE', ''))}")
            
            # Preguntar si descargar
            respuesta = input("   ¿Descargar datos? (s/n): ").lower()
            if respuesta == 's':
                descargar_estacion_especifica(
                    estacion['id'], 
                    estacion['nombre'], 
                    estacion['categoria']
                )
        else:
            print("❌ No tiene variables")
        
        time.sleep(2)

if __name__ == "__main__":
    print("📥 DESCARGADOR SNIRH-ANA (CORREGIDO)")
    print("📍 Categorías: 'Climática' e 'Hidrométrica'")
    print("=" * 50)
    
    # Opción 1: Explorar cuencas primero (RECOMENDADO)
    explorar_cuencas()
    
    # Opción 2: Probar estaciones conocidas
    # prueba_estaciones_conocidas()
    
    # Opción 3: Descargar cuenca específica
    # descargar_cuenca_completa("019", "Cuenca Azangaro", ["Climática"])
    
    # Opción 4: Descargar estación específica
    # descargar_estacion_especifica("37", "Ananea 000826", "Climática")