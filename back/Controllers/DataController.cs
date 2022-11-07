using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using RetoTecnicoXAl.Models;
using RetoTecnicoXAl.Services;
using System.Data;
using System.Net;
using Dapper;

namespace RetoTecnicoXAl.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : ControllerBase
    {
        readonly IDbConnection _con;
        readonly IConfiguration _config;
        
        public DataController(IConfiguration configuration)
        {
            _config = configuration;
            _con = new SqlConnection(configuration["ConnectionStrings:DefaultConnection"]);
        }

        [HttpGet]
        [Route("respuestas/CorrectasEIncorrectas")]
        public async Task<IActionResult> ObtenerRespuestasCorrectasEIncorrectas()
        {
            var res = await obtenerDatos();


            var contestadas = res.items.Where(i => i.is_answered).Count();
            var noContestadas = res.items.Where(i => !i.is_answered).Count();
            var menosVistas = res.items.Min(i => i.view_count);
            var respuestaVieja = res.items.Min(i => i.last_activity_date);
            var respuestaActual = res.items.Max(i => i.last_activity_date);
            
            var max = 1;
            foreach(var item in res.items)
            {
                max = item.owner.reputation > max ? item.owner.reputation : max;
            }


            var respuestas = new Respuestas
            {
                Contestadas = contestadas,
                NoContestadas = noContestadas,
                MayorReputacion = max,
                MenosVistas = menosVistas,
                RespuestaActual = respuestaActual,
                RespuestaVieja = respuestaVieja
            };

            return Ok(respuestas);
        }
        
        [HttpGet]
        [Route("aerolinea/aeropuertoMayorFlujo")]
        public async Task<IEnumerable<Aeropuerto>> aeropuertoMayorFlujo()
        {
            string queryBD = "SELECT TOP 1 count([aeropuerto].[dbo].[aeropuerto].nombre_aerolinea) AS numero_movimientos, [aeropuerto].[dbo].[aeropuerto].aeropuerto_mayor_flujo FROM [aeropuerto].[dbo].[vuelos] INNER JOIN [aeropuerto].[dbo].[aeropuerto] ON [aeropuerto].[dbo].[aeropuerto].id_aeropuerto = [aeropuerto].[dbo].[vuelos].id_aeropuerto GROUP BY [aeropuerto].[dbo].[aeropuerto].nombre_aerolinea;";
            try
            {

                using (IDbConnection con = new SqlConnection(_config["ConnectionStrings:DefaultConnection"]))
                {
                    con.Open();
                    return con.Query<Aeropuerto>(sql: queryBD);
                }

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }
        
        [HttpGet]
        [Route("aerolinea/aerolineaMayorFlujo")]
        public async Task<IEnumerable<Aeropuerto>> aerolineaMayorFlujo()
        {
            string queryBD = "SELECT TOP 1 count([aeropuerto].[dbo].[aerolineas].nombre_aerolinea) AS numero_movimientos, [aeropuerto].[dbo].[aerolineas].nombre_aerolinea FROM [aeropuerto].[dbo].[vuelos] INNER JOIN [aeropuerto].[dbo].[aerolineas] ON [aeropuerto].[dbo].[aerolineas].id_aerolinea = [aeropuerto].[dbo].[vuelos].id_aerolinea GROUP BY [aeropuerto].[dbo].[aerolineas].nombre_aerolinea;";
            try
            {

                using (IDbConnection con = new SqlConnection(_config["ConnectionStrings:DefaultConnection"]))
                {
                    con.Open();
                    return con.Query<Aeropuerto>(sql: queryBD);
                }

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }
        
        [HttpGet]
        [Route("aerolinea/diasMasVuelos")]
        public async Task<IEnumerable<Aeropuerto>> diasMasVuelos()
        {
            string queryBD = "SELECT TOP 1 count([aeropuerto].[dbo].[vuelos].dia) AS numero_movimientos, [aeropuerto].[dbo].[vuelos].dia FROM [aeropuerto].[dbo].[vuelos] GROUP BY [aeropuerto].[dbo].[vuelos].dia;";
            try
            {

                using (IDbConnection con = new SqlConnection(_config["ConnectionStrings:DefaultConnection"]))
                {
                    con.Open();
                    return con.Query<Aeropuerto>(sql: queryBD);
                }

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }
        
        [HttpGet]
        [Route("aerolinea/diasDosVuelos")]
        public async Task<IEnumerable<Aeropuerto>> diasDosVuelos()
        {
            string queryBD = "WITH cc as (SELECT count([aeropuerto].[dbo].[vuelos].dia) AS numero_movimientos, [aeropuerto].[dbo].[vuelos].dia, [aeropuerto].[dbo].[vuelos].id_aerolinea FROM [aeropuerto].[dbo].[vuelos] GROUP BY [aeropuerto].[dbo].[vuelos].dia, [aeropuerto].[dbo].[vuelos].id_aerolinea) SELECT cc.numero_movimientos, [aeropuerto].[dbo].[aerolineas].nombre_aerolinea AS aerolinea_mayor_flujo FROM cc INNER JOIN [aeropuerto].[dbo].[aerolineas] ON [aeropuerto].[dbo].[aerolineas].id_aerolinea = cc.id_aerolinea WHERE cc.numero_movimientos > 2;";
            try
            {

                using (IDbConnection con = new SqlConnection(_config["ConnectionStrings:DefaultConnection"]))
                {
                    con.Open();
                    return con.Query<Aeropuerto>(sql: queryBD);
                }

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }
        
        private async Task<Root> obtenerDatos()
        {
            var url = "https://api.stackexchange.com/2.2/search?order=desc&sort=activity&intitle=perl&site=stackoverflow";
            HttpWebRequest request = WebRequest.CreateHttp(url);

            request.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => true;
            // If required by the server, set the credentials.
            request.Credentials = CredentialCache.DefaultCredentials;
            request.AutomaticDecompression = DecompressionMethods.GZip;


            // Get the response.
            WebResponse response = request.GetResponse();
            // Display the status.
            Console.WriteLine(((HttpWebResponse)response).StatusDescription);
            var res = new Root();
            using (Stream dataStream = response.GetResponseStream())
            {
                StreamReader reader = new StreamReader(dataStream);

                var stringRes = reader.ReadToEnd();
                return JsonConvert.DeserializeObject<Root>(stringRes);
            }

            response.Close();
            return res;
        }
    }
}