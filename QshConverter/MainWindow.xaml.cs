using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Forms;
using QshLib;
using MessageBox = System.Windows.MessageBox;

namespace QshConverter
{
    // Указываете путь к директории с историческими данными в формате .qsh (по умолчанию C:\QshStorage)
    // Указываете путь к хранилищу SAT, где будут сохраняться сконвертированные данные (по умолчанию C:\Users\%UserName%\Documents\SoftAlgoTrade\Storage)
    // Тип исходных исторических данных: Сделки - Deals.gsh или Ордерлог - OrdLog.qsh

    public partial class MainWindow : Window
    {
        private DateTime _startTime;

        public MainWindow()
        {
            InitializeComponent();

            SatStoragePath.Text = $"{Environment.GetFolderPath(Environment.SpecialFolder.Personal)}\\SoftAlgoTrade\\Storage";
        }

        private void ConvertBtnClick(object sender, RoutedEventArgs e)
        {
            _startTime = DateTime.Now;

            var gshStoragePath = QshStoragePath.Text;
            var satStoragePath = SatStoragePath.Text;

            if (!Directory.Exists(gshStoragePath)) return;

            ConvertBtn.IsEnabled = false;

            TbProgress.Text = "0% ...";

            Task.Run(() =>
            {
                var converter = new QshLib.QshConverter(satStoragePath, GetSourceDataType());

                converter.TotalProgress += progress => Dispatcher.Invoke(() => TbProgress.Text = progress);

                converter.Convert(gshStoragePath);

                Dispatcher.Invoke(() =>
                {
                    MessageBox.Show($"Data is successfully converted!\nTime:{(DateTime.Now - _startTime)}", "Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                    ConvertBtn.IsEnabled = true;
                });
            });
        }

        private SourceDataType GetSourceDataType() => Dispatcher.Invoke(() =>
        {
            var sdt = SourceDataType.AuxInfo;

            if ((bool) OrdLog.IsChecked) sdt = SourceDataType.OrderLog;
            else if ((bool) Deals.IsChecked) sdt = SourceDataType.Deals;

            return sdt;
        });

        private void SatStorageClick(object sender, RoutedEventArgs e)
        {
            var openFolderDialog = new FolderBrowserDialog();
            openFolderDialog.ShowDialog();
            SatStoragePath.Text = openFolderDialog.SelectedPath;
        }

        private void QsgStorageClick(object sender, RoutedEventArgs e)
        {
            var openFolderDialog = new FolderBrowserDialog();
            openFolderDialog.ShowDialog();
            QshStoragePath.Text = openFolderDialog.SelectedPath;
        }
    }
}
