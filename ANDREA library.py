# -*- coding: utf-8 -*-
"""
Created on Wed Oct 15 00:23:35 2025

@author: ASUS
"""
#############################    get_data 
import requests
import pandas as pd
import numpy as np
import warnings
import json
import re
import time
from typing import Union, List, Optional
from multiprocessing.pool import ThreadPool
from threading import Lock
from tqdm import tqdm

class ANDREA:
    """
    Cliente para la base de datos ANDREA de la ANA Perú
    Con mapeo automático de nombres a códigos reales
    """
    
    BASE_URL = "https://snirh.ana.gob.pe/ANDREA"
    DEFAULT_TIMEOUT = 30
    MAX_THREADS = 5

    def __init__(self):
        self.session = requests.Session()
        self._setup_headers()
        self.lock = Lock()
        # Cache para mapeo de nombres a códigos
        self._name_to_code_cache = {}
        # Cache para estaciones por cuenca
        self._basin_stations_cache = {}
    
    def _setup_headers(self):
        self.session.headers.update({
            'Content-Type': 'application/json; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': f'{self.BASE_URL}/Integrado.aspx',
            'Origin': self.BASE_URL,
            'X-Requested-With': 'XMLHttpRequest'
        })

    @staticmethod
    def get_data(station_codes: Union[str, List[str], int, np.integer] = None,
                basin_name: str = None,
                data_type: str = 'both',
                start_date: Optional[str] = None,
                end_date: Optional[str] = None,
                threads: int = MAX_THREADS,
                source: str = 'ANDREAF') -> pd.DataFrame:
        """
        Obtiene datos de series temporales
        """
        andrea = ANDREA()
        
        # Determinar qué estaciones descargar
        station_codes_to_download = andrea._resolve_station_codes(station_codes, basin_name, data_type, source)
        
        if not station_codes_to_download:
            warnings.warn("No se encontraron estaciones para descargar")
            return pd.DataFrame()
        
        print(f"📥 Descargando {len(station_codes_to_download)} estaciones...")
        
        # 🔥 NUEVO: Convertir nombres a códigos reales
        real_codes = andrea._map_names_to_real_codes(station_codes_to_download)
        
        if not real_codes:
            warnings.warn("No se pudieron encontrar códigos reales para las estaciones")
            return pd.DataFrame()
            
        return andrea._fetch_data(real_codes, data_type, start_date, end_date, threads)

    def _map_names_to_real_codes(self, station_codes):
        """Convierte nombres/temp_codes a códigos reales de ANDREA"""
        real_codes = []
        
        for code in station_codes:
            # Convertir a string primero para manejar tanto int como str
            code_str = str(code)
            
            # Si ya es un código numérico real, usarlo directamente
            if code_str.isdigit() and len(code_str) <= 6:  # Los códigos ANDREA son típicamente 1-6 dígitos
                real_codes.append(code_str)
                continue
                
            # Si es un código temporal (TEMPXXXX) o nombre, buscar código real
            real_code = self._find_real_station_code(code_str)
            if real_code:
                real_codes.append(real_code)
                print(f"   🔍 Mapeo: '{code_str}' -> '{real_code}'")
            else:
                warnings.warn(f"No se pudo encontrar código real para: {code_str}")
        
        return real_codes

    def _find_real_station_code(self, station_identifier):
        """Busca el código real de una estación por nombre o código temporal"""
        # Verificar cache primero
        if station_identifier in self._name_to_code_cache:
            return self._name_to_code_cache[station_identifier]
        
        # Buscar en todas las cuencas
        cuencas = self._get_basins_from_api()
        
        for cuenca in cuencas:
            codigo_cuenca = cuenca.get('CodigoUH', '')
            estaciones = self._get_stations_from_api(codigo_cuenca)
            
            for estacion in estaciones:
                nombre_api = self._clean_name(estacion.get('NOM', ''))
                codigo_real = str(estacion.get('ID', ''))  # Asegurar que es string
                
                # Buscar por coincidencia de nombre
                if self._names_match(station_identifier, nombre_api):
                    self._name_to_code_cache[station_identifier] = codigo_real
                    return codigo_real
                
                # También buscar por código si es numérico
                if station_identifier.isdigit() and codigo_real == station_identifier:
                    self._name_to_code_cache[station_identifier] = codigo_real
                    return codigo_real
        
        # Si no se encuentra, devolver None
        return None

    def _names_match(self, name1, name2):
        """Verifica si dos nombres de estación coinciden"""
        if not name1 or not name2:
            return False
            
        # Limpiar nombres
        clean1 = self._clean_name(str(name1)).lower()
        clean2 = self._clean_name(str(name2)).lower()
        
        # Coincidencia exacta o por palabras clave
        if clean1 in clean2 or clean2 in clean1:
            return True
            
        # Coincidencia por palabras significativas
        words1 = set(clean1.split())
        words2 = set(clean2.split())
        common_words = words1.intersection(words2)
        
        # Si comparten al menos 2 palabras significativas
        significant_words = [w for w in common_words if len(w) > 3]
        return len(significant_words) >= 2

    def _clean_name(self, text):
        """Limpia nombre de estación - versión mejorada"""
        if not text:
            return ""
        
        # Remover etiquetas HTML y contenido entre <>
        clean = re.sub(r'<[^>]+>', '', text)
        
        # Remover 'Estación' y otros prefijos comunes
        clean = re.sub(r'^(Estación|Est\.|Station|span|class|text|muted|medium)\s*', '', clean, flags=re.IGNORECASE)
        
        # Remover números largos que parecen códigos (más de 5 dígitos)
        clean = re.sub(r'\s+\d{5,}', '', clean)
        
        # Remover caracteres especiales pero mantener espacios y letras
        clean = re.sub(r'[^\w\sáéíóúñÁÉÍÓÚÑ]', ' ', clean)
        
        # Remover espacios múltiples
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        return clean

    def _get_basins_from_api(self):
        """Obtiene cuencas desde la API"""
        url = f"{self.BASE_URL}/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": "", "pBuscar": "", "pTipo": "UH"}
        
        try:
            response = self.session.post(url, json=data, timeout=self.DEFAULT_TIMEOUT)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result:
                    return json.loads(result['d'])
        except Exception as e:
            warnings.warn(f"Error obteniendo cuencas: {e}")
        
        return []

    def _get_stations_from_api(self, basin_code):
        """Obtiene estaciones de una cuenca desde API"""
        url = f"{self.BASE_URL}/Integrado.aspx/EstacionBuscar"
        data = {"pCodigoUH": basin_code, "pBuscar": "", "pTipo": "EST"}
        
        try:
            response = self.session.post(url, json=data, timeout=self.DEFAULT_TIMEOUT)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result:
                    return json.loads(result['d'])
        except Exception as e:
            warnings.warn(f"Error obteniendo estaciones: {e}")
        
        return []

    def _resolve_station_codes(self, station_codes, basin_name, data_type, source):
        """Resuelve qué códigos de estación descargar"""
        codes_to_download = set()
        
        # Agregar estaciones específicas si se proporcionan
        if station_codes is not None:
            if isinstance(station_codes, (str, int, np.integer)):
                codes_to_download.add(str(station_codes))
            elif hasattr(station_codes, '__len__'):
                codes_to_download.update(str(code) for code in station_codes)
        
        # Agregar estaciones de la cuenca si se especifica
        if basin_name is not None:
            basin_stations = self._get_stations_by_basin(basin_name, data_type, source)
            if not basin_stations.empty:
                basin_codes = basin_stations['code'].astype(str).tolist()  # Asegurar strings
                codes_to_download.update(basin_codes)
                print(f"🏞️  Cuenca '{basin_name}': {len(basin_codes)} estaciones {data_type}")
        
        return list(codes_to_download)

    def _get_stations_by_basin(self, basin_name, data_type, source):
        """Obtiene estaciones de una cuenca desde la API"""
        # Buscar la cuenca por nombre
        cuencas = self._get_basins_from_api()
        cuenca_encontrada = None
        
        for cuenca in cuencas:
            nombre_cuenca = cuenca.get('NombreUH', '').lower()
            if basin_name.lower() in nombre_cuenca:
                cuenca_encontrada = cuenca
                break
        
        if not cuenca_encontrada:
            warnings.warn(f"No se encontró la cuenca: {basin_name}")
            return pd.DataFrame()
        
        codigo_cuenca = cuenca_encontrada.get('CodigoUH', '')
        estaciones = self._get_stations_from_api(codigo_cuenca)
        
        if not estaciones:
            return pd.DataFrame()
        
        # Procesar estaciones
        estaciones_procesadas = []
        
        for estacion in estaciones:
            # Obtener detalles completos de la estación
            detalles = self._get_station_details(estacion.get('ID', ''))
            
            if detalles:
                # Determinar tipo de datos basado en las variables
                variables = self._get_station_variables(estacion.get('ID', ''))
                tipo_estacion = self._determine_station_type(variables, data_type)
                
                if tipo_estacion:  # Solo incluir si coincide con el tipo solicitado
                    estacion_info = {
                        'code': str(estacion.get('ID', '')),  # Asegurar string
                        'name': self._clean_name(estacion.get('NOM', '')),
                        'data_type': tipo_estacion,
                        'category': estacion.get('CAT', ''),
                        'basin': cuenca_encontrada.get('NombreUH', ''),
                        'operator': detalles.get('OPERADOR', ''),
                        'latitude': detalles.get('COOLATITUD', 0),
                        'longitude': detalles.get('COOLONGITUD', 0),
                        'elevation': detalles.get('ALTITUD', 0)
                    }
                    estaciones_procesadas.append(estacion_info)
        
        return pd.DataFrame(estaciones_procesadas)

    def _get_station_details(self, station_id):
        """Obtiene detalles completos de una estación"""
        url = f"{self.BASE_URL}/index.aspx/EstacionSeleccionar"
        data = {"pIDEstacion": station_id}
        
        try:
            response = self.session.post(url, json=data, timeout=self.DEFAULT_TIMEOUT)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result:
                    detalles = json.loads(result['d'])
                    if detalles and 'Lista' in detalles and len(detalles['Lista']) > 0:
                        return detalles['Lista'][0]
        except Exception as e:
            warnings.warn(f"Error obteniendo detalles de estación {station_id}: {e}")
        
        return None

    def _determine_station_type(self, variables, requested_type):
        """Determina el tipo de estación basado en sus variables"""
        if not variables:
            return None
        
        # CORREGIDO: Buscar tanto con tilde como sin tilde
        has_precipitation = any(
            'PRECIPITACION' in v.get('NOMBRE', '').upper() or 
            'PRECIPITACIÓN' in v.get('NOMBRE', '').upper() 
            for v in variables
        )
        has_flow = any('CAUDAL' in v.get('NOMBRE', '').upper() for v in variables)
        
        if requested_type == 'both':
            if has_precipitation and has_flow:
                return 'both'
            elif has_precipitation:
                return 'precipitation'
            elif has_flow:
                return 'flow'
        elif requested_type == 'prec' and has_precipitation:
            return 'precipitation'
        elif requested_type == 'flow' and has_flow:
            return 'flow'
        
        return None

    @staticmethod
    def list_stations(station_type: str = 'both', 
                     basin_name: str = '', 
                     source: str = 'ANDREAF') -> pd.DataFrame:
        """Lista estaciones disponibles desde la API"""
        andrea = ANDREA()
        return andrea._get_stations_from_api_direct(station_type, basin_name)

    def _get_stations_from_api_direct(self, station_type: str, basin_name: str) -> pd.DataFrame:
        """Obtiene estaciones directamente desde la API"""
        print("🔍 Obteniendo estaciones desde la API de ANDREA...")
        
        # Obtener todas las cuencas
        cuencas = self._get_basins_from_api()
        todas_estaciones = []
        
        # Si se especifica una cuenca, buscar solo esa
        if basin_name:
            cuencas_filtradas = [c for c in cuencas if basin_name.lower() in c.get('NombreUH', '').lower()]
            if not cuencas_filtradas:
                warnings.warn(f"No se encontró la cuenca: {basin_name}")
                return pd.DataFrame()
            cuencas = cuencas_filtradas
        
        print(f"📊 Procesando {len(cuencas)} cuencas...")
        
        for cuenca in tqdm(cuencas, desc="Procesando cuencas"):
            codigo_cuenca = cuenca.get('CodigoUH', '')
            nombre_cuenca = cuenca.get('NombreUH', '')
            
            estaciones = self._get_stations_from_api(codigo_cuenca)
            
            for estacion in estaciones:
                station_id = estacion.get('ID', '')
                
                # Obtener detalles y variables para determinar el tipo
                detalles = self._get_station_details(station_id)
                variables = self._get_station_variables(station_id)
                
                if detalles and variables:
                    tipo_estacion = self._determine_station_type(variables, station_type)
                    
                    if tipo_estacion:  # Solo incluir si coincide con el tipo solicitado
                        estacion_info = {
                            'code': str(station_id),  # Asegurar string
                            'name': self._clean_name(estacion.get('NOM', '')),
                            'data_type': tipo_estacion,
                            'category': estacion.get('CAT', ''),
                            'basin': nombre_cuenca,
                            'operator': detalles.get('OPERADOR', ''),
                            'data_quality': 'Primario',  # Valor por defecto
                            'latitude': detalles.get('COOLATITUD', 0),
                            'longitude': detalles.get('COOLONGITUD', 0),
                            'elevation': detalles.get('ALTITUD', 0)
                        }
                        todas_estaciones.append(estacion_info)
            
            time.sleep(0.1)  # Pausa breve para no saturar la API
        
        if not todas_estaciones:
            return pd.DataFrame()
            
        df = pd.DataFrame(todas_estaciones)
        
        # Filtrar por tipo si es específico
        if station_type == 'prec':
            df = df[df['data_type'] == 'precipitation']
        elif station_type == 'flow':
            df = df[df['data_type'] == 'flow']
        
        # Ordenar columnas
        column_order = ['code', 'name', 'data_type', 'category', 'basin', 
                       'operator', 'data_quality', 'latitude', 'longitude', 'elevation']
        available_cols = [col for col in column_order if col in df.columns]
        
        print(f"✅ Se encontraron {len(df)} estaciones")
        return df[available_cols].reset_index(drop=True)

    def _fetch_data(self, station_codes, data_type, start_date, end_date, threads):
        """Descarga datos de las estaciones"""
        if len(station_codes) > 1:
            return self._fetch_parallel(station_codes, data_type, start_date, end_date, threads)
        else:
            return self._fetch_single_station(station_codes[0], data_type, start_date, end_date)

    def _fetch_parallel(self, station_codes, data_type, start_date, end_date, threads):
        """Descarga paralela de múltiples estaciones"""
        def process_station(station_code):
            try:
                return self._fetch_single_station(station_code, data_type, start_date, end_date)
            except Exception as e:
                warnings.warn(f"Error en estación {station_code}: {str(e)}")
                return pd.DataFrame()
        
        with ThreadPool(min(threads, len(station_codes))) as pool:
            results = list(tqdm(pool.imap(process_station, station_codes), 
                              total=len(station_codes),
                              desc="Descargando datos"))
        
        valid_results = [df for df in results if not df.empty]
        if valid_results:
            combined_df = pd.concat(valid_results, axis=1)
            return combined_df.sort_index()
        else:
            return pd.DataFrame()

    def _fetch_single_station(self, station_code, data_type, start_date, end_date):
        """Descarga datos de una sola estación"""
        variables = self._get_station_variables(station_code)
        if not variables:
            return pd.DataFrame()
        
        station_data = {}
        
        for variable in variables:
            var_name = variable.get('NOMBRE', '')
            var_upper = var_name.upper()
            
            # CORREGIDO: Buscar tanto con tilde como sin tilde
            if data_type == 'prec':
                if 'PRECIPITACION' not in var_upper and 'PRECIPITACIÓN' not in var_upper:
                    continue
            if data_type == 'flow' and 'CAUDAL' not in var_upper:
                continue
            
            # Solo datos diarios
            if "DIA" in var_upper or "DIAR" in var_upper:
                daily_data = self._download_daily_series(station_code, variable)
                if daily_data is not None:
                    col_name = f"{station_code}_{var_name}"
                    station_data[col_name] = daily_data
        
        if not station_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(station_data)
        
        # Filtrar por fecha
        if start_date:
            start_date = pd.to_datetime(start_date)
            df = df[df.index >= start_date]
        if end_date:
            end_date = pd.to_datetime(end_date)
            df = df[df.index <= end_date]
            
        return df.sort_index()

    def _get_station_variables(self, station_code):
        """Obtiene variables disponibles para una estación"""
        url = f"{self.BASE_URL}/index.aspx/VariableListarxEstacion"
        data = {"pIDEstacion": station_code, "pTipo": "DMA"}
        
        try:
            response = self.session.post(url, json=data, timeout=self.DEFAULT_TIMEOUT)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result:
                    return json.loads(result['d'])
        except Exception as e:
            warnings.warn(f"Error obteniendo variables: {e}")
        
        return []

    def _download_daily_series(self, station_code, variable_info):
        """Descarga serie diaria de una variable"""
        url = f"{self.BASE_URL}/ServicioGeneral.asmx/SerieDatos"
        data = {
            "IDEstacion": station_code,
            "IDOperador": variable_info.get('IDOPERADOR', ''),
            "IDVariable": variable_info.get('ID', ''),
            "TipoSerie": "DIARIA",
            "pIDEstudio": variable_info.get('IDESTUDIO', ''),
            "IDUsuario": 0,
            "IDTipoRegistro": 1
        }
        
        try:
            response = self.session.post(url, json=data, timeout=60)
            if response.status_code == 200:
                result = response.json()
                if 'd' in result and result['d']:
                    return self._process_series_data(result['d'])
        except Exception as e:
            warnings.warn(f"Error descargando datos: {e}")
        
        return None

    def _process_series_data(self, raw_data):
        """Procesa datos de series temporales"""
        processed_data = []
        
        for item in raw_data:
            if isinstance(item, list) and len(item) >= 2:
                date_str, value = item[0], item[1]
                
                try:
                    # Convertir fecha
                    date = pd.to_datetime(date_str)
                    
                    # Convertir valor
                    if value is None or value == '':
                        numeric_value = None
                    else:
                        if isinstance(value, str):
                            value = value.strip().replace(',', '.')
                        numeric_value = float(value)
                    
                    processed_data.append({'date': date, 'value': numeric_value})
                    
                except (ValueError, TypeError):
                    continue
        
        if not processed_data:
            return None
            
        df = pd.DataFrame(processed_data)
        return df.set_index('date')['value']

# Funciones de conveniencia
def get_data(station_codes=None, basin_name=None, data_type='both', 
             start_date=None, end_date=None, threads=5, source='ANDREAF'):
    return ANDREA.get_data(station_codes, basin_name, data_type, start_date, end_date, threads, source)

def get_prec(station_codes=None, basin_name=None, start_date=None, end_date=None, threads=5):
    return ANDREA.get_data(station_codes, basin_name, 'prec', start_date, end_date, threads)

def get_flow(station_codes=None, basin_name=None, start_date=None, end_date=None, threads=5):
    return ANDREA.get_data(station_codes, basin_name, 'flow', start_date, end_date, threads)

def list_stations(station_type='both', basin_name='', source='ANDREAF'):
    return ANDREA.list_stations(station_type, basin_name, source)

# Ejemplo de uso
#if __name__ == "__main__":
    # Listar estaciones de la cuenca Biabo
    #print("🔍 Buscando estaciones en la cuenca Biabo...")
    #estaciones_biabo = list_stations(basin_name='Biabo')
    #print(estaciones_biabo)
    
    # Descargar datos de Biabo
    #print("\n📥 Descargando datos de Biabo...")
    #datos_biabo = get_data(basin_name='Biabo')
    #print(datos_biabo.head())
    
# Datos de un período específico
datos_biabo_2020_2023 = get_prec(
    basin_name='Biabo',
    start_date='1970-01-01',
    end_date='1975-12-31',
    threads=3
)


######################   PreProcessing

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from tqdm import tqdm


class PreProcessing:

    @staticmethod
    def stations_filter(data, n_years=10, missing_percentage=5, start_date=False, end_date=False):
        """
        A composed method to filter stations. 
        
        First, the method filters the stations data by the Start Date and the End Date, it its passed. After that, the 
        is selected only the stations with at least a defined number of years between the first date and the last date
        of the station. At the end is selected the stations that contains at least one window of data with the number of
        years and a maximum missing data percentage. 

        Parameters
        ----------
        data : pandas DataFrame
            A Pandas daily DataFrame with DatetimeIndex where each column corresponds to a station.
        n_years: int, default 10
            The minimum number of years of registered data for the station between the first date and the end date.
        missing_percentage: int, default 5
             The maximum missing data percentage in a window with n_years.
             A number between 0 and 100
        start_date : int, float, str, default False
            The desired start date for the output DataFrame.
            See: pandas.to_datetime documentation if have doubts about the date format
        end_date: int, float, str, default False
            The desired end date for the output DataFrame.
            See: pandas.to_datetime documentation if have doubts about the date format

        Returns
        -------
        data : pandas DataFrame
            A pandas DataFrame with only the filtered stations
        """
        # If the start and/or end date is given this step selects the temporal window in the dataset
        if start_date != False and end_date != False:
            start_date = pd.to_datetime([start_date])
            end_date = pd.to_datetime([end_date])
            data = data.loc[start_date[0]:end_date[0]]
        elif start_date:
            start_date = pd.to_datetime([start_date])
            data = data.loc[start_date[0]:]
        elif end_date:
            end_date = pd.to_datetime([end_date])
            data = data.loc[:end_date[0]]

        # This step selects the stations with at least n_years between the first date and the last date of the station.
        stations = []
        for column in data.columns:
            series = data[column]
            series_drop = series.dropna()
            if len(series_drop) > 0:
                # CORREGIDO: Calcular años usando días en lugar de timedelta64('Y')
                days = (series_drop.index[-1] - series_drop.index[0]).days
                years = days / 365.25  # 365.25 para considerar años bisiestos
                if years >= n_years:
                    stations.append(column)
        data = data[stations]

        # This last step looks for at least a temporal window with until missing_percentage of missing data.
        stations = []
        state = 0
        for column in tqdm(data.columns):
            series = data[column]
            series_drop = series.dropna()
            periods = []
            start1 = series_drop.index[0]
            finish1 = 0
            for i in range(len(series_drop)):
                if i != 0 and (series_drop.index[i] - series_drop.index[i - 1]).days != 1:
                    finish1 = series_drop.index[i - 1]
                    # CORREGIDO: Calcular intervalo en años usando días
                    interval_days = (finish1 - start1).days
                    interval_years = interval_days / 365.25
                    periods.append(dict(Start=start1, Finish=finish1, Interval=interval_years))
                    start1 = series_drop.index[i]
                    finish1 = 0
            finish1 = series_drop.index[-1]
            # CORREGIDO: Calcular intervalo en años usando días
            interval_days = (finish1 - start1).days
            interval_years = interval_days / 365.25
            periods.append(dict(Start=start1, Finish=finish1, Interval=interval_years))
            periods = pd.DataFrame(periods)
            if len(periods[periods['Interval'] >= n_years]) > 0:
                stations.append(column)
            else:
                j = 0
                aux = 0
                while j < len(periods) and aux == 0:
                    j += 1
                    if periods['Start'][j] + relativedelta(years=n_years) <= periods['Finish'][periods.index[-1]]:
                        series_period = series.loc[
                                        periods['Start'][j]:periods['Start'][j] + relativedelta(years=n_years)]
                        missing = series_period.isnull().sum() / len(series_period)
                        if missing <= missing_percentage / 100 and aux == 0:
                            aux = 1
                            stations.append(column)
                    else:
                        aux = 1
            state += 1
        data = data[stations]
        return data

    @staticmethod
    def daily_to_monthly(data, method='sum'):
        """
        Transform a time series of daily data into a time series monthly data.

        In the conversion process a month with a day missing data is considered as a missing month.

        Parameters
        ----------
        data : pandas DataFrame
            A Pandas daily DataFrame with DatetimeIndex where each column corresponds to a station.
        method: str, default sum
            The method used to convert. If 'sum', the monthly data will be the sum of the daily data. If 'mean', the
            monthly data will be the mean of the daily data.

        Returns
        -------
        monthly_data : pandas DataFrame
            The  monthly pandas DataFrame
        """

        monthly_data = pd.DataFrame()
        for column in data.columns:
            series = data[column]
            if method == 'sum':
                monthly_series = series.groupby(pd.Grouper(freq='1MS')).sum().to_frame()
            elif method == 'mean':
                monthly_series = series.groupby(pd.Grouper(freq='1MS')).mean().to_frame()
            else:
                raise Exception('Please select a valid method.')
            missing = series.isnull().groupby(pd.Grouper(freq='1MS')).sum().to_frame()
            to_drop = missing.loc[missing[column] > 0]  # A month with a missing data is a missing month
            monthly_series = monthly_series.drop(index=to_drop.index).sort_index()
            data_index = pd.date_range(monthly_series.index[0], monthly_series.index[-1], freq='MS')
            monthly_series = monthly_series.reindex(data_index)
            monthly_data = pd.concat([monthly_data, monthly_series], axis=1)
        return monthly_data
    


# SEPARAR por tipo de dato antes de filtrar
from get_data import get_prec, get_flow

# Descargar por separado
prec_data = get_prec(basin_name='Biabo', start_date='2000-01-01', end_date='2016-12-31')
flow_data = get_flow(basin_name='Biabo', start_date='2000-01-01', end_date='2016-12-31')

# Aplicar filtros CON PARÁMETROS DIFERENTES
filtered_prec = PreProcessing.stations_filter(
    prec_data, 
    n_years=10,           
    missing_percentage=5,  # Más estricto para precipitación
    start_date='2005-01-01',
    end_date='2015-12-31'
)

filtered_flow = PreProcessing.stations_filter(
    flow_data, 
    n_years=8,            # Menos años para caudal (son más raros)
    missing_percentage=10, # Más tolerante con datos faltantes
    start_date='2005-01-01',
    end_date='2015-12-31'
)

# Convertir a mensual
monthly_prec = PreProcessing.daily_to_monthly(filtered_prec, method='sum')    # Suma para precipitación
monthly_flow = PreProcessing.daily_to_monthly(filtered_flow, method='mean')   # Promedio para caudal

print(f"Estaciones de precipitación filtradas: {len(filtered_prec.columns)}")
print(f"Estaciones de caudal filtradas: {len(filtered_flow.columns)}")

# O descargar todo pero aplicar criterios diferentes después
all_data = get_data(basin_name='Biabo', start_date='2000-01-01', end_date='2016-12-31')

# Identificar qué columnas son precipitación y cuáles caudal
prec_columns = [col for col in all_data.columns if 'PRECIPITACION' in col.upper()]
flow_columns = [col for col in all_data.columns if 'CAUDAL' in col.upper()]

# Separar los datos
prec_data = all_data[prec_columns]
flow_data = all_data[flow_columns]


########################### plot

import numpy as np
import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
from math import ceil, log

# Importar HoloViews (opcional - solo si está instalado)
try:
    import holoviews as hv
    from holoviews import opts
    hv.extension('bokeh')
    HOLOVIEWS_AVAILABLE = True
except ImportError:
    HOLOVIEWS_AVAILABLE = False

class Plot:

    @staticmethod
    def fdc(data, y_log_scale=True):
        """
        Make a flow duration curve plot.

        Parameters
        ----------
        data : pandas DataFrame
            A Pandas daily DataFrame with DatetimeIndex where each column corresponds to a station.
        y_log_scale : boolean, default True
            Defines if the the plotting y-axis will be in the logarithmic scale.

        Returns
        -------
        fig : plotly Figure
        """

        fig = go.Figure()
        y_max = 0
        for name in data.columns:
            series = data[name].dropna()
            n = len(series)
            y = np.sort(series)
            y = y[::-1]
            if y_max < y.max():
                y_max = y.max()
            x = (np.arange(1, n + 1) / n) * 100
            fig.add_trace(go.Scatter(x=x, y=y, mode='lines', name=name))

        if y_log_scale:
            ticks = 10 ** np.arange(1, ceil(log(y_max, 10)) + 1, 1)
            ticks[-1:] += 1
            fig.update_layout(yaxis=dict(
                tickmode='array', tickvals=ticks, dtick=2), yaxis_type="log")
        fig.update_layout(xaxis=dict(tickmode='array', tickvals=np.arange(0, 101, step=10)))
        return fig

    @staticmethod
    def gantt(data, monthly=True):
        """
        Make a Gantt plot, which shows the temporal data availability for each station.

        Parameters
        ----------
        data : pandas DataFrame
            A Pandas daily DataFrame with DatetimeIndex where each column corresponds to a station.
        monthly : boolean, default True
            Defines if the availability count of the data will be monthly to obtain a more fluid graph.

        Returns
        -------
        fig : plotly Figure
        """

        date_index = pd.date_range(data.index[0], data.index[-1], freq='D')
        data = data.reindex(date_index)
        periods = []
        for column in data.columns:
            series = data[column]
            if monthly:
                missing = series.isnull().groupby(pd.Grouper(freq='1MS')).sum().to_frame()
                series_drop = missing.loc[missing[column] < 7]  # A MONTH WITHOUT 7 DATA IS CONSIDERED A MISSING MONTH
                DELTA = 'M'
            else:
                series_drop = series.dropna()
                DELTA = 'D'
            if series_drop.shape[0] > 1:
                task = column
                resource = 'Available data'
                start = str(series_drop.index[0].year) + '-' + str(series_drop.index[0].month) + '-' + str(
                    series_drop.index[0].day)
                finish = 0
                for i in range(len(series_drop)):
                    if i != 0 and round((series_drop.index[i] - series_drop.index[i - 1]) / np.timedelta64(1, DELTA),
                                        0) != 1:
                        finish = str(series_drop.index[i - 1].year) + '-' + str(
                            series_drop.index[i - 1].month) + '-' + str(
                            series_drop.index[i - 1].day)
                        periods.append(dict(Task=task, Start=start, Finish=finish, Resource=resource))
                        start = str(series_drop.index[i].year) + '-' + str(series_drop.index[i].month) + '-' + str(
                            series_drop.index[i].day)
                        finish = 0
                finish = str(series_drop.index[-1].year) + '-' + str(series_drop.index[-1].month) + '-' + str(
                    series_drop.index[-1].day)
                periods.append(dict(Task=task, Start=start, Finish=finish, Resource=resource))
            else:
                print('Station {} has no months with significant data'.format(column))
        periods = pd.DataFrame(periods)
        start_year = periods['Start'].apply(lambda x: int(x[:4])).min()
        finish_year = periods['Start'].apply(lambda x: int(x[:4])).max()
        colors = {'Available data': 'rgb(0,191,255)'}
        fig = ff.create_gantt(periods, colors=colors, index_col='Resource', show_colorbar=True, showgrid_x=True,
                              showgrid_y=True, group_tasks=True)

        fig.layout.xaxis.tickvals = pd.date_range('1/1/' + str(start_year), '12/31/' + str(finish_year + 1), freq='2AS')
        fig.layout.xaxis.ticktext = pd.date_range('1/1/' + str(start_year), '12/31/' + str(finish_year + 1),
                                                  freq='2AS').year
        return fig

    @staticmethod
    def spatial_stations(list_stations, mapbox_access_token):
        """
        Make a spatial plot of the stations.

        Parameters
        ----------
        list_stations : pandas DataFrame
            A Pandas DataFrame that must contain Latitude, Longitude, Name, and Code columns.
        mapbox_access_token : str
            Mapbox access toke, which can be obtained at https://account.mapbox.com/access-tokens/

        Returns
        -------
        fig : plotly Figure
        """

        if ('Latitude' not in list_stations.columns) or ('Longitude' not in list_stations.columns):
            raise Exception('Longitude and Latitude columns are required')
        list_stations['Text'] = 'Name: ' + list_stations.Name + '<br>Code: ' + list_stations.Code
        list_stations[['Latitude', 'Longitude']] = list_stations[['Latitude', 'Longitude']].apply(pd.to_numeric,
                                                                                                  errors='coerce')

        # Creating the Figure
        fig = go.Figure(go.Scattermapbox(lat=list_stations.Latitude.to_list(), lon=list_stations.Longitude.to_list(),
                                         mode='markers', marker=go.scattermapbox.Marker(size=5),
                                         text=list_stations.Text.to_list()))

        # Updating the layout
        fig.update_layout(autosize=True, hovermode='closest',
                          mapbox=dict(accesstoken=mapbox_access_token, bearing=0,
                                      center=dict(lat=list_stations.Latitude.sum() / len(list_stations),
                                                  lon=list_stations.Longitude.sum() / len(list_stations)),
                                      pitch=0, zoom=4))

        return fig

    # ========== NUEVOS MÉTODOS CON HOLOVIEWS ==========

    @staticmethod
    def hydro_ts(data, station_name=None, data_type='flow', title=None, **kwargs):
        """
        Create interactive time series plot using HoloViews.
        
        Parameters
        ----------
        data : pandas DataFrame
            Time series data with DatetimeIndex
        station_name : str, optional
            Specific station to plot. If None, plots all stations.
        data_type : str, default 'flow'
            Type of data: 'flow' for discharge, 'prec' for precipitation
        title : str, optional
            Plot title
        **kwargs : additional styling options
        
        Returns
        -------
        holoviews Curve or Overlay
        """
        if not HOLOVIEWS_AVAILABLE:
            raise ImportError("HoloViews is required. Install with: pip install holoviews bokeh")
        
        # Select data to plot
        if station_name and station_name in data.columns:
            plot_data = data[[station_name]].copy()
            station_display = station_name.split('_', 1)[1] if '_' in station_name else station_name
        else:
            plot_data = data.copy()
            station_display = "Multiple Stations"
        
        # Set default parameters based on data type
        defaults = {
            'flow': {
                'ylabel': 'River Discharge [m³/s]',
                'color': 'navy',
                'line_width': 2,
                'height': 400,
                'width': 800
            },
            'prec': {
                'ylabel': 'Precipitation [mm]',
                'color': 'steelblue', 
                'line_width': 1,
                'height': 400,
                'width': 800
            }
        }
        
        config = defaults.get(data_type, defaults['flow'])
        config.update(kwargs)
        
        # Create plots for each station
        curves = []
        colors = ['navy', 'firebrick', 'forestgreen', 'darkorange', 'purple']
        
        for i, column in enumerate(plot_data.columns):
            series = plot_data[column].dropna()
            if len(series) == 0:
                continue
                
            # Prepare data for HoloViews
            df_plot = pd.DataFrame({
                'Date': series.index,
                'Value': series.values
            })
            
            # Extract station name for display
            display_name = column.split('_', 1)[1] if '_' in column else column
            
            # Create curve
            curve = hv.Curve(df_plot, 'Date', 'Value', label=display_name)
            
            # Apply styling
            curve = curve.opts(
                width=config['width'],
                height=config['height'],
                ylabel=config['ylabel'],
                color=colors[i % len(colors)],
                line_width=config['line_width'],
                show_grid=True,
                tools=['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset', 'save'],
                active_tools=['wheel_zoom']
            )
            
            curves.append(curve)
        
        if not curves:
            raise ValueError("No valid data to plot")
        
        # Create title
        if title is None:
            station_info = station_display if station_name else f"{len(curves)} Stations"
            title = f'{station_info} - {data_type.capitalize()} Time Series'
        
        # Combine curves
        if len(curves) == 1:
            plot = curves[0].opts(title=title)
        else:
            plot = hv.Overlay(curves).opts(title=title)
        
        # Apply additional styling
        style = dict(
            Title=dict(text_font_size='14pt', align='center'),
            Axis=dict(axis_line_color='black', axis_label_text_font_size='12pt'),
            TickLabel=dict(text_font_size='10pt'),
        )
        
        return plot.opts(opts.Curve(**style))

    @staticmethod
    def hydro_ts_comparison(data_dict, data_type='flow', title=None, **kwargs):
        """
        Compare multiple time series from different DataFrames.
        
        Parameters
        ----------
        data_dict : dict
            Dictionary with {label: dataframe} pairs
        data_type : str, default 'flow'
            Type of data being compared
        title : str, optional
            Plot title
            
        Returns
        -------
        holoviews Overlay
        """
        if not HOLOVIEWS_AVAILABLE:
            raise ImportError("HoloViews is required. Install with: pip install holoviews bokeh")
        
        curves = []
        colors = ['navy', 'firebrick', 'forestgreen', 'darkorange', 'purple', 'brown']
        
        for i, (label, df) in enumerate(data_dict.items()):
            if len(df.columns) == 0:
                continue
                
            # Use first column for comparison
            series = df[df.columns[0]].dropna()
            if len(series) == 0:
                continue
            
            df_plot = pd.DataFrame({
                'Date': series.index,
                'Value': series.values
            })
            
            curve = hv.Curve(df_plot, 'Date', 'Value', label=label)
            curve = curve.opts(
                color=colors[i % len(colors)],
                line_width=2,
                show_grid=True,
                tools=['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset', 'save']
            )
            
            curves.append(curve)
        
        if not curves:
            raise ValueError("No valid data to plot")
        
        # Set defaults
        defaults = {
            'flow': {'ylabel': 'River Discharge [m³/s]'},
            'prec': {'ylabel': 'Precipitation [mm]'}
        }
        
        config = defaults.get(data_type, defaults['flow'])
        config.update({
            'width': 800,
            'height': 400,
            'title': title or f'{data_type.capitalize()} Comparison'
        })
        config.update(kwargs)
        
        plot = hv.Overlay(curves).opts(
            opts.Curve(**config),
            opts.Curve(
                Title=dict(text_font_size='14pt'),
                Axis=dict(axis_label_text_font_size='12pt'),
                TickLabel=dict(text_font_size='10pt')
            )
        )
        
        return plot

    @staticmethod
    def monthly_hydroplot(data, data_type='flow', title=None, **kwargs):
        """
        Create monthly distribution plot (boxplot) using HoloViews.
        
        Parameters
        ----------
        data : pandas DataFrame
            Daily time series data
        data_type : str, default 'flow'
            Type of data: 'flow' or 'prec'
        title : str, optional
            Plot title
            
        Returns
        -------
        holoviews BoxWhisker
        """
        if not HOLOVIEWS_AVAILABLE:
            raise ImportError("HoloViews is required. Install with: pip install holoviews bokeh")
        
        # Prepare data for monthly analysis
        plot_data = []
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for column in data.columns:
            series = data[column].dropna()
            for date, value in series.items():
                plot_data.append({
                    'Month': month_names[date.month - 1],
                    'Value': value,
                    'Station': column.split('_', 1)[1] if '_' in column else column
                })
        
        if not plot_data:
            raise ValueError("No valid data for monthly plot")
            
        df_plot = pd.DataFrame(plot_data)
        
        # Create boxplot
        boxes = hv.BoxWhisker(df_plot, 'Month', 'Value', label='Monthly Distribution')
        
        # Set defaults
        defaults = {
            'flow': {
                'ylabel': 'River Discharge [m³/s]',
                'title': 'Monthly Flow Distribution',
                'color': 'lightsteelblue'
            },
            'prec': {
                'ylabel': 'Precipitation [mm]', 
                'title': 'Monthly Precipitation Distribution',
                'color': 'lightblue'
            }
        }
        
        config = defaults.get(data_type, defaults['flow'])
        config.update({
            'width': 800,
            'height': 400,
            'title': title or config['title'],
            'show_grid': True,
            'box_fill_color': config['color'],
            'invert_axes': False,
            'tools': ['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset', 'save']
        })
        config.update(kwargs)
        
        plot = boxes.opts(opts.BoxWhisker(**config))
        
        return plot

    @staticmethod
    def is_holoviews_available():
        """
        Check if HoloViews is available for interactive plotting.
        
        Returns
        -------
        bool
            True if HoloViews is installed
        """
        return HOLOVIEWS_AVAILABLE

    # ========== MÉTODOS AUXILIARES ==========

    @staticmethod
    def check_data_requirements(data):
        """
        Check if data meets basic requirements for plotting.
        
        Parameters
        ----------
        data : pandas DataFrame
            Data to check
            
        Returns
        -------
        bool
            True if data is valid for plotting
        """
        if data is None or data.empty:
            raise ValueError("Data is empty or None")
        
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("Data index must be DatetimeIndex")
        
        if len(data.columns) == 0:
            raise ValueError("No columns found in data")
        
        return True

    @staticmethod
    def get_plotting_colors(n_colors):
        """
        Get a list of distinct colors for plotting.
        
        Parameters
        ----------
        n_colors : int
            Number of colors needed
            
        Returns
        -------
        list
            List of color names
        """
        base_colors = ['navy', 'firebrick', 'forestgreen', 'darkorange', 'purple', 
                      'brown', 'pink', 'gray', 'olive', 'cyan']
        
        if n_colors <= len(base_colors):
            return base_colors[:n_colors]
        else:
            # Generate additional colors if needed
            return base_colors + ['teal', 'maroon', 'gold', 'lightblue'] * (n_colors // len(base_colors) + 1)

    @staticmethod
    def export_plot(plot, filename, format='png', width=800, height=400):
        """
        Export plot to file.
        
        Parameters
        ----------
        plot : holoviews or plotly object
            Plot to export
        filename : str
            Output filename
        format : str, default 'png'
            Export format
        width : int, default 800
            Export width
        height : int, default 400
            Export height
        """
        if HOLOVIEWS_AVAILABLE and isinstance(plot, (hv.Curve, hv.Overlay, hv.BoxWhisker)):
            # Export HoloViews plot
            hv.save(plot, filename, fmt=format)
        elif hasattr(plot, 'write_image'):
            # Export Plotly plot
            plot.write_image(filename, width=width, height=height)
        else:
            raise ValueError("Unsupported plot type for export")
        
        print(f"Plot exported to: {filename}")
    
# Ejemplo completo de uso
from get_data import get_data, get_prec, get_flow, list_stations
from preprocessing import PreProcessing

# 1. Obtener datos
print("📥 Descargando datos de Biabo...")
data = get_data(basin_name='Biabo', start_date='2000-01-01', end_date='2020-12-31')

# 2. Filtrar estaciones
print("🔍 Filtrando estaciones...")
filtered_data = PreProcessing.stations_filter(
    data, 
    n_years=5, 
    missing_percentage=10,
    start_date='2005-01-01',
    end_date='2015-12-31'
)

# 3. Convertir a mensual
print("📊 Convirtiendo a mensual...")
monthly_data = PreProcessing.daily_to_monthly(filtered_data, method='sum')

# 4. Visualizaciones
print("📈 Generando gráficos...")

# Curva de duración
fdc_fig = Plot.fdc(monthly_data, y_log_scale=True, data_type='prec')
fdc_fig.show()

# Disponibilidad de datos
gantt_fig = Plot.gantt(filtered_data, monthly=True)
gantt_fig.show()

# Series temporales
ts_fig = Plot.time_series_andrea(monthly_data, title="Precipitación Mensual - Biabo", data_type='prec')
ts_fig.show()

# Boxplot mensual
box_fig = Plot.monthly_boxplot(monthly_data, data_type='prec')
box_fig.show()

# Mapa de estaciones (necesitas primero listar las estaciones)
print("🗺️ Generando mapa...")
stations_info = list_stations(basin_name='Biabo')
map_fig = Plot.spatial_stations_andrea(stations_info)
map_fig.show()


######################## save
import os
import pandas as pd
import locale

class SaveAs:

    @staticmethod
    def asc_daily_prec(data, path_save):
        """
        Save each column of the precipitation stations DataFrame into a ".txt" file in the ASCII standard.

        Parameters
        ----------
        data : pandas DataFrame
            A Pandas daily DataFrame with DatetimeIndex where each column corresponds to a station.
        path_save: string
            The computer location where the ".txt" files will be saved.

        Returns
        -------
        """

        if not os.path.exists(path_save):
            os.makedirs(path_save)
        stations = list(data.columns.values)
        for i in range(len(stations)):
            file_name = str(stations[i])
            while len(file_name) < 8:
                file_name = '0' + file_name
            file_name = file_name + '.txt'
            df = data[stations[i]].to_frame().round(2)
            df = df.dropna()
            date_index = pd.date_range(df.index[0], df.index[-1], freq='D')
            df = df.reindex(date_index)
            df = df.fillna(-1.00)
            df = df.round(decimals=2)
            arq = open(os.path.join(os.path.join(os.getcwd(), path_save), file_name), 'w')
            list_dado = []
            for j in df.index:
                dado = df[stations[i]][j]
                list_dado.append(dado)
                arq.write('{:>6}{:>6}{:>6}{:>12}\n'.format(j.day, j.month, j.year, format(dado, '.2f')))
            arq.close()

    @staticmethod
    def asc_daily_flow(data, path_save):
        """
        Save each column of the flow stations DataFrame into a ".txt" file in the ASCII standard.

        Parameters
        ----------
        data : pandas DataFrame
            A Pandas daily DataFrame with DatetimeIndex where each column corresponds to a station.
        path_save: string
            The computer location where the ".txt" files will be saved.

        Returns
        -------
        """

        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        if not os.path.exists(path_save):
            os.makedirs(path_save)
        stations = list(data.columns.values)
        for i in range(len(stations)):
            file_name = str(stations[i])
            while len(file_name) < 8:
                file_name = '0' + file_name
            file_name = file_name + '.txt'
            df = data[stations[i]].to_frame().round(6)
            df = df.dropna()
            date_index = pd.date_range(df.index[0], df.index[-1], freq='D')
            df = df.reindex(date_index)
            df = df.fillna(-1.00)
            df = df.round(decimals=6)
            arq = open(os.path.join(os.path.join(os.getcwd(), path_save), file_name), 'w')
            for j in df.index:
                dado = df[stations[i]][j]
                dado = locale.format_string('%.6f', dado, True)
                if len(dado) < 11:
                    arq.write('{:>6}{:>6}{:>6}{:>16}\n'.format(j.day, j.month, j.year, dado))
                else:
                    arq.write('{:>6}{:>6}{:>6}{:>18}\n'.format(j.day, j.month, j.year, dado))
            arq.close()

    @staticmethod
    def to_csv(data, path_save, filename="hydro_data.csv"):
        """
        Save data to CSV file.
        
        Parameters
        ----------
        data : pandas DataFrame
            Data to save
        path_save : str
            Directory path to save the file
        filename : str, default "hydro_data.csv"
            Name of the CSV file
        """
        if not os.path.exists(path_save):
            os.makedirs(path_save)
        filepath = os.path.join(path_save, filename)
        data.to_csv(filepath)
        print(f"Data saved to: {filepath}")

    @staticmethod
    def to_excel(data, path_save, filename="hydro_data.xlsx"):
        """
        Save data to Excel file.
        
        Parameters
        ----------
        data : pandas DataFrame
            Data to save
        path_save : str
            Directory path to save the file  
        filename : str, default "hydro_data.xlsx"
            Name of the Excel file
        """
        if not os.path.exists(path_save):
            os.makedirs(path_save)
        filepath = os.path.join(path_save, filename)
        data.to_excel(filepath)
        print(f"Data saved to: {filepath}")