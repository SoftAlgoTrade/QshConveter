using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using QScalp;
using QScalp.History.Reader;
using SAT.Storage;
using SAT.Trading;
using Security = SAT.Trading.Security;

namespace QshLib
{
    //Конвертер .qsh файлов
    public class QshConverter
    {
        private readonly object _lock = new object();

        //Размер партии для многопоточной конвертации
        private const int DataPart = 700;

        //Сообщения при конвертации
        public event Action<string> TotalProgress;
        public event Action<string> Exception;

        private readonly Storage _storage;

        //Поля для расчета прогресса выполнения конвертации
        private DateTime _startTime;
        private int _count;
        private int _total;
        private int _percent;

        //Выбранный тип данных для конвертации
        private readonly SourceDataType _sourceDataType;

        //Таймфремы для автоматической конвертации тиков в свечки
        private readonly List<TimeSpan> _timeFrames = new List<TimeSpan>();

        //Добавить таймфреймы для автоматической конвертации тиков в свечки
        public void AddTimeFrames(List<TimeSpan> timeFrames)
        {
            _timeFrames.Clear();
            _timeFrames.AddRange(timeFrames);
        }

        public QshConverter(string satStoragePath, SourceDataType sourceDataType)
        {
            _sourceDataType = sourceDataType;

            if (!Directory.Exists(satStoragePath)) Directory.CreateDirectory(satStoragePath);

            _storage = new Storage(satStoragePath);
        }

        //Поиск файлов в .qsh хранилище и их конвертация
        public void Convert(string gshStoragePath) => Convert(GetQshFiles(gshStoragePath));

        //Конвертация файлов
        public void Convert(List<string> files)
        {
            _startTime = DateTime.Now;

            var filesList = new List<string>();

            foreach (var file in files)
                if (CheckFileName(file)) filesList.Add(file);

            _total = filesList.Count;

            //Разбиваем на партии
            var parts = new List<List<string>>();
            var buffer = new List<string>();

            for (int i = 0; i < filesList.Count; i++)
            {
                buffer.Add(filesList[i]);

                if (buffer.Count >= DataPart)
                {
                    parts.Add(buffer.ToList());
                    buffer.Clear();
                }
            }

            parts.Add(buffer);

            foreach (var part in parts)
                ConvertPart(part);
        }

        //Многопоточная конвертации партии .qsh файлов
        private void ConvertPart(List<string> files)
        {
            var array = new Task[files.Count];

            for (int i = 0; i < files.Count; i++)
            {
                var file = files[i];

                array[i] = new Task((() =>
                {
                    try
                    {
                        var totalList = new List<List<Trade>>();

                        if (_sourceDataType == SourceDataType.OrderLog)
                            totalList = OrdLogReader(file);

                        if (_sourceDataType == SourceDataType.Deals)
                            totalList.Add(DealsReader(file));

                        foreach (var list in totalList)
                        {
                            if (list.Count > 1)
                            {
                                var sec = list[0].Security;

                                _storage.Save(list, sec);

                                for (int j = 0; j < _timeFrames.Count; j++)
                                {
                                    var tf = _timeFrames[j];

                                    using (var createCandle = new CandleBuilder(tf))
                                    {
                                        var candles = createCandle.Build(list);
                                        _storage.Save(candles, tf, sec);
                                    }
                                }

                                list.Clear();
                            }
                        }

                        lock (_lock)
                            Progress();
                    }
                    catch (Exception e)
                    {
                        Exception?.Invoke(e.ToString());
                    }
                }));
            }

            foreach (var task in array)
                task.Start();

            Task.WaitAll(array);
        }

        //Расчет прогресса
        private void Progress()
        {
            try
            {
                var percent = ++_count*100/_total;

                if (percent > 0 && percent != _percent)
                {
                    _percent = percent;

                    var endPeriod = (DateTime.Now - _startTime).TotalSeconds*100/_percent;

                    TotalProgress?.Invoke($"{_percent}% - {(_startTime.AddSeconds(endPeriod) - DateTime.Now).ToString("hh\\:mm\\:ss")}");
                }
            }
            catch (Exception e)
            {
                Exception?.Invoke(e.ToString());
            }
        }

        //Поиск .qsh файлов в директории
        private static List<string> GetQshFiles(string gshStoragePath)
        {
            var pathfiles = new List<string>();

            var dirs = Directory.GetDirectories(gshStoragePath);

            foreach (var files in dirs.Select(dir => Directory.GetFiles(dir, "*.qsh")))
            {
                pathfiles.AddRange(files);
            }

            return pathfiles;
        }

        //Формирование инструмента
        private static Security SecurityConverter(QScalp.Security security)
        {
            var type = SecurityType.Futures;

            if (security.Ticker.EndsWith("TOM") || security.Ticker.EndsWith("TOD")) type = SecurityType.Options;

            var name = !string.IsNullOrEmpty(security.AuxCode) ? $"{security.AuxCode}-{security.CName}" : $"{security.CName}";

            var sec = new Security
            {
                ID = security.Id,
                Symbol = security.Ticker,
                Name = name,
                Type = type,
                Tick = (decimal) security.Step
            };

            return sec;
        }

        //Определение типа данных по названиям файлов
        private bool CheckFileName(string filePath)
        {
            var name = Path.GetFileNameWithoutExtension(filePath);

            if (_sourceDataType == SourceDataType.OrderLog)
            {
                if (!name.Contains("OrdLog")) return false;
            }
            else if (_sourceDataType == SourceDataType.Deals)
            {
                if (!name.Contains("Deals")) return false;
            }
            else if (_sourceDataType == SourceDataType.Quotes)
            {
                if (!name.Contains("Quotes")) return false;
            }

            return true;
        }

        //Определение направления сделки
        private static AggressorSide DealTypeConverter(DealType dealType)
        {
            var aggressorSide = AggressorSide.None;

            if (dealType == DealType.Buy) aggressorSide = AggressorSide.Buy;
            else if (dealType == DealType.Sell) aggressorSide = AggressorSide.Sell;

            return aggressorSide;
        }

        //Ордерлог
        private List<List<Trade>> OrdLogReader(string filePath)
        {
            var list = new List<List<Trade>> {new List<Trade>()};

            using (var qr = QshReader.Open(filePath))
            {
                IOrdLogStream stream = null;

                for (var i = 0; i < qr.StreamCount; i++)
                {
                    var logStream = qr[i] as IOrdLogStream;

                    if (logStream != null)
                    {
                        stream = logStream;
                        break;
                    }
                }

                if (stream == null) return list;

                var security = SecurityConverter(stream.Security);

                using (var converter = new OrderLogToMarketDepth(security))
                {
                    converter.NewTick += tick =>
                    {
                        var lastDate = DateTime.MaxValue;

                        var lastList = list[list.Count - 1];

                        if (lastList.Count > 0)
                            lastDate = lastList[lastList.Count - 1].Time.Date;

                        if (tick.Time.Date > lastDate)
                            list.Add(new List<Trade>());

                        list[list.Count - 1].Add(tick);
                    };

                    stream.Handler += ol => converter.Add(OrdLogConverter(ol));

                    while (qr.CurrentDateTime != DateTime.MaxValue)
                        qr.Read(true);
                }
            }

            return list;
        }

        private static OrderLog OrdLogConverter(OrdLogEntry ol)
        {
            var states = new HashSet<OrderLogStates>();

            if (ol.Flags.HasFlag(OrdLogFlags.NonSystem) || ol.Flags.HasFlag(OrdLogFlags.Snapshot))
                states.Add(OrderLogStates.NonSystem);
            if (ol.Flags.HasFlag(OrdLogFlags.Add))
                states.Add(OrderLogStates.Add);
            if (ol.Flags.HasFlag(OrdLogFlags.Fill))
                states.Add(OrderLogStates.Fill);
            if (ol.Flags.HasFlag(OrdLogFlags.Buy))
                states.Add(OrderLogStates.Buy);
            if (ol.Flags.HasFlag(OrdLogFlags.Sell))
                states.Add(OrderLogStates.Sell);

            return new OrderLog
            {
                States = states,
                Time = ol.DateTime,
                OrderId = ol.OrderId,
                Price = ol.Price,
                Volume = ol.Amount,
                VolumeRest = ol.AmountRest,
                TradeId = ol.DealId,
                TradePrice = ol.DealPrice
            };
        }

        //Сделки с направлением
        private List<Trade> DealsReader(string filePath)
        {
            var list = new List<Trade>();

            using (var qr = QshReader.Open(filePath))
            {
                try
                {
                    for (var i = 0; i < qr.StreamCount; i++)
                    {
                        var stream = qr[i] as IDealsStream;

                        if (stream == null) return list;

                        var security = SecurityConverter(stream.Security);

                        var step = (decimal) stream.Security.Step;

                        stream.Handler += dael =>
                        {
                            list.Add(new Trade
                            {
                                ID = dael.Id,
                                Price = dael.Price*step,
                                Volume = dael.Volume,
                                Time = dael.DateTime,
                                Security = security,
                                Aggressor = DealTypeConverter(dael.Type)
                            });
                        };
                    }

                    while (qr.CurrentDateTime != DateTime.MaxValue)
                    {
                        qr.Read(true);
                    }
                }
                catch (Exception e)
                {
                    Debug.WriteLine(e.ToString());
                }
            }

            return list;
        }
    }
}
