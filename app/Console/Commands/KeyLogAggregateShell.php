<?php
namespace App\Console\Commands;

use Aws\Athena\AthenaClient;
use Aws\S3\S3Client;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Exception;
use PHPExcel_IOFactory;

/**
 * KeyLogAggregateShell.
 *
 * @author Naganuma Yu
 */
class KeyLogAggregateShell extends Command
{

    /**
     * S3Client.
     */
    private $s3Client;

    /**
     * AthenaClient.
     */
    private $athenaClient;

    /**
     * work path.
     */
    private $workPath;

    /**
     * hash value.
     */
    private $hash;

    /**
     * file name.
     */
    private $file;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'KeyLogAggregateShell';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'The Shell of Aggregating Keyboard Logs for IT Solution';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();

        $this->s3Client = new S3Client([
            'region' => 'ap-northeast-1',
            'version' => '2006-03-01'
        ]);

        $this->athenaClient = new AthenaClient([
            'region' => 'ap-northeast-1',
            'version' => '2017-05-18'
        ]);
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        Log::info($this->signature . ':start');

        // タイムゾーン
        date_default_timezone_set('Asia/Tokyo');

        // 作業ディレクトリ作成
        $this->hash = md5(time());
        $this->workPath = base_path('work' . '/' . $this->hash . '/');
        mkdir($this->workPath);

        // テンプレートExcelコピー
        $this->file = 'KeyTypingAggregationInfo_' . date('YmdHis') . '.xlsx';
        copy(base_path('resources/views/excel/KeyTypingAggregationInfo.xlsx'), $this->workPath . $this->file);

        // パーティション読み込み
        $this->__loadPartition();

        // タイピングスピード集計
        $this->__aggregateTypingSpeed();

        // タイピングスピード集計
        $this->__aggregateTypingCountEveryHour();

        // 頻出単語集計
        $this->__aggregateFrequentWord();

        // 結果ファイルS3アップロード
        $this->uploadToS3('Athena', $this->workPath . $this->file);

        // 作業ディレクトリ削除
        $this->removeDir($this->workPath);
    }

    /**
     * パーティション読み込み.
     *
     * @param string $table
     *            テーブル名
     * @return void
     */
    private function __loadPartition()
    {
        $query = view('sql.loadPartition')->render();
        $this->__executeQuery($query);
    }

    /**
     * コンピュータ名取得.
     *
     * @return array コンピュータ名の配列
     */
    private function __getComputerName()
    {
        $query = view('sql.getComputerName')->render();
        $this->__executeQuery($query);
    }

    /**
     * タイピングスピード集計.
     */
    private function __aggregateTypingSpeed()
    {
        // クエリ実行
        $query = view('sql.aggregateTypingSpeed')->render();
        $id = $this->__executeQuery($query);

        // 結果取得
        $this->downloadFromS3('Athena/tmp/' . $this->hash . '/' . $id . '.csv', $this->workPath . 'TypingSpeed.csv');

        // Excelに出力
        $objReader = PHPExcel_IOFactory::createReader('Excel2007');
        // Excelにグラフが含まれているフラグ
        $objReader->setIncludeCharts(TRUE);

        $book = $objReader->load($this->workPath . $this->file);
        // 最初のシートを作業対象に選択
        $book->setActiveSheetIndex(0);
        $sheet = $book->getActiveSheet();

        // 結果ファイルオープン
        $fp = fopen($this->workPath . 'TypingSpeed.csv', 'r');
        // ヘッダー行は読み飛ばす
        fgetcsv($fp);

        // 表開始行
        $dataRow = '11';
        while (($data = fgetcsv($fp)) !== FALSE) {

            // コンピュータ名
            $sheet->setCellValue('A' . $dataRow, $data[0]);
            // タイピング数値
            $sheet->setCellValue('C' . $dataRow, $data[2]);

            $dataRow ++;
        }
        // 結果ファイルクローズ
        fclose($fp);

        $writer = PHPExcel_IOFactory::createWriter($book, "Excel2007");
        // Excelにグラフが含まれているフラグ
        $writer->setIncludeCharts(TRUE);
        $writer->save($this->workPath . $this->file);
    }

    /**
     * タイピング数時間別集計.
     */
    private function __aggregateTypingCountEveryHour()
    {
        // クエリ実行
        $query = view('sql.aggregateTypingCountEveryHour')->render();
        $id = $this->__executeQuery($query);

        // 結果取得
        $this->downloadFromS3('Athena/tmp/' . $this->hash . '/' . $id . '.csv', $this->workPath . 'TypingCountEveryHour.csv');

        // Excelに出力
        $objReader = PHPExcel_IOFactory::createReader('Excel2007');
        // Excelにグラフが含まれているフラグ
        $objReader->setIncludeCharts(TRUE);

        $book = $objReader->load($this->workPath . $this->file);
        // 2番目のシートを作業対象に選択
        $book->setActiveSheetIndex(1);
        $sheet = $book->getActiveSheet();

        // 結果ファイルオープン
        $fp = fopen($this->workPath . 'TypingCountEveryHour.csv', 'r');
        // ヘッダー行は読み飛ばす
        fgetcsv($fp);

        // 表開始行
        $dataRow = 52;
        while (($data = fgetcsv($fp)) !== FALSE) {

            $hours = explode(',', $data[1]);
            $cnts = explode(',', $data[2]);

            // コンピュータ名
            $sheet->setCellValue('A' . $dataRow, $data[0]);

            for ($i = 0; $i < count($hours); $i ++) {
                // タイピング数(時間帯別)
                switch ((int) $hours[$i]) {
                    case 9:
                        // 09:00～10:00
                        $sheet->setCellValue('C' . $dataRow, $cnts[$i]);
                        break;
                    case 10:
                        // 10:00～11:00
                        $sheet->setCellValue('D' . $dataRow, $cnts[$i]);
                        break;
                    case 11:
                        // 11:00～12:00
                        $sheet->setCellValue('E' . $dataRow, $cnts[$i]);
                        break;
                    case 12:
                        // 12:00～13:00
                        $sheet->setCellValue('F' . $dataRow, $cnts[$i]);
                        break;
                    case 13:
                        // 13:00～14:00
                        $sheet->setCellValue('G' . $dataRow, $cnts[$i]);
                        break;
                    case 14:
                        // 14:00～15:00
                        $sheet->setCellValue('H' . $dataRow, $cnts[$i]);
                        break;
                    case 15:
                        // 15:00～16:00
                        $sheet->setCellValue('I' . $dataRow, $cnts[$i]);
                        break;
                    case 16:
                        // 16:00～17:00
                        $sheet->setCellValue('J' . $dataRow, $cnts[$i]);
                        break;
                    case 17:
                        // 17:00～18:00
                        $sheet->setCellValue('K' . $dataRow, $cnts[$i]);
                        break;
                    default:
                        break;
                }
            }
            $dataRow++;
        }
        // 結果ファイルクローズ
        fclose($fp);

        $writer = PHPExcel_IOFactory::createWriter($book, "Excel2007");
        // Excelにグラフが含まれているフラグ
        $writer->setIncludeCharts(TRUE);
        $writer->save($this->workPath . $this->file);
    }

    /**
     * 頻出単語集計.
     */
    private function __aggregateFrequentWord()
    {
        // クエリ実行
        $lowerLimitLength = config('app.aggregateFrequentWord.lowerLimitLength', 10);
        $query = view('sql.aggregateFrequentWord', ['lowerLimitLength' => $lowerLimitLength])->render();
        $id = $this->__executeQuery($query);

        // 結果取得
        $this->downloadFromS3('Athena/tmp/' . $this->hash . '/' . $id . '.csv', $this->workPath . 'FrequentWord.csv');

        // Excelに出力
        $objReader = PHPExcel_IOFactory::createReader('Excel2007');
        // Excelにグラフが含まれているフラグ
        $objReader->setIncludeCharts(TRUE);

        $book = $objReader->load($this->workPath . $this->file);
        // 最初のシートを作業対象に選択
        $book->setActiveSheetIndex(0);
        $sheet = $book->getActiveSheet();

        // 結果ファイルオープン
        $fp = fopen($this->workPath . 'FrequentWord.csv', 'r');
        // ヘッダー行は読み飛ばす
        fgetcsv($fp);

        // 表開始行
        $dataRow = '11';
        while (($data = fgetcsv($fp)) !== FALSE) {
            $words = explode(',', $data[1]);
            $frequencies = explode(',', $data[2]);

            $frequentWord = '';
            // 単語は最大10個まで
            for ($i = 0; $i < count($words) && $i < 10; $i ++) {
                $frequentWord .= $indention . $words[$i] . ' (' . $frequencies[$i] . ')' .PHP_EOL;
            }
            // 頻出単語
            $sheet->setCellValue('D' . $dataRow, $frequentWord);

            $dataRow ++;
        }

        // 結果ファイルクローズ
        fclose($fp);

        $writer = PHPExcel_IOFactory::createWriter($book, "Excel2007");
        // Excelにグラフが含まれているフラグ
        $writer->setIncludeCharts(TRUE);
        $writer->save($this->workPath . $this->file);
    }

    /**
     * クエリ実行.
     *
     * @param string $query
     *            クエリ
     * @param boolean $isSelect
     *            SELECT実行フラグ
     * @return string AthenaクエリID
     */
    private function __executeQuery($query)
    {
        Log::debug('execute query : ' . $query);

        $s3WorkPath = 's3://' . env('S3_BUCKET_NAME') . '/Athena/tmp/' . $this->hash . '/';

        $startQueryExecutionResult = $this->athenaClient->startQueryExecution([
            'QueryString' => $query,
            'ResultConfiguration' => [
                'EncryptionConfiguration' => [
                    'EncryptionOption' => 'SSE_S3'
                ],
                'OutputLocation' => $s3WorkPath
            ]
        ]);
        Log::info(var_export($startQueryExecutionResult, true));

        $id = $startQueryExecutionResult['QueryExecutionId'];

        // クエリの結果を最大30分待つ
        $startTime = time() / (1000 * 60);
        while ((time() / (1000 * 60)) - $startTime <= 30) {
            sleep(env('ATHENA_SELECT_SLEEP_TIME', 10));
            $getQueryExecutionResult = $this->athenaClient->getQueryExecution([
                'QueryExecutionId' => $id
            ]);

            $status = $getQueryExecutionResult['QueryExecution']['Status']['State'];
            if ($status !== 'QUEUED' && $status !== 'RUNNING') {
                break;
            }
        }

        Log::info(var_export($getQueryExecutionResult, true));

        if (! $status === 'SUCCEEDED') {
            throw new Exception('Athenaクエリの実行に失敗しました : ' . var_export($getQueryExecutionResult, true), 1);
        }

        return $id;
    }

    /**
     * S3にURL集計に関連するファイルをアップロードする
     *
     * @param string $s3Key
     *            S3キー
     * @param string $filePath
     *            ファイルパス
     * @throws Exception 想定外のエラー
     * @return void
     */
    public function uploadToS3($s3Key, $filePath)
    {
        $md5sum = md5_file($filePath, false);
        $result = $this->s3Client->putObject(array(
            'Bucket' => env('S3_BUCKET_NAME'),
            'ACL' => 'private',
            'Key' => $s3Key . '/' . basename($filePath),
            'SourceFile' => $filePath,
            'ContentType' => mime_content_type($filePath)
        ));
        Log::info(var_export($result, true));

        // md5でファイルが正常にS3に届いたか確認する
        if ($md5sum === str_replace('"', '', $result['ETag'])) {
            return true;
        } else {
            throw new Exception('S3のアップロードに失敗しました : ' . var_export($result, true), 1);
        }
    }

    /**
     * S3からURL集計に関連するファイルをダウンロードする
     *
     * @param string $s3Key
     *            S3キー
     * @param string $to
     *            ダウンロード先
     * @throws Exception 想定外のエラー
     */
    public function downloadFromS3($s3Key, $to)
    {
        $result = $this->s3Client->getObject([
            'Bucket' => env('S3_BUCKET_NAME'),
            'Key' => $s3Key,
            'SaveAs' => $to
        ]);
        Log::info(var_export($result, true));
    }

    /**
     * 再帰的にディレクトリを削除する。
     *
     * @param string $dir
     *            ディレクトリ名（フルパス）
     */
    function removeDir($dir)
    {
        $cnt = 0;

        $handle = opendir($dir);
        if (! $handle) {
            return;
        }

        while (false !== ($item = readdir($handle))) {
            if ($item === "." || $item === "..") {
                continue;
            }

            $path = $dir . DIRECTORY_SEPARATOR . $item;

            if (is_dir($path)) {
                // 再帰的に削除
                $cnt = $cnt + removeDir($path);
            } else {
                // ファイルを削除
                unlink($path);
            }
        }
        closedir($handle);

        // ディレクトリを削除
        if (! rmdir($dir)) {
            return;
        }
    }
}
